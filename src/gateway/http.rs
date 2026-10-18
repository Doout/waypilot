//! Streamable HTTP ingress and operator endpoints.
use super::{rpc_error, valid_request, Actor, Gateway, Session};
use crate::upstream::{MAX_MESSAGE, VERSIONS};
use anyhow::Result;
use axum::{
    body::{to_bytes, Body},
    extract::{DefaultBodyLimit, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tracing::Instrument;

impl Gateway {
    pub(super) fn router(self: &Arc<Self>) -> Router {
        Router::new()
            .route("/healthz", get(readiness))
            .route("/mcp", post(http_mcp).get(http_get).delete(http_delete))
            .route("/v1/prewarm", post(http_prewarm))
            .route("/admin/stats", get(admin_stats))
            .route("/admin/users", get(admin_users))
            .route("/admin/users/update", post(admin_update))
            .route("/admin/invalidate", post(admin_invalidate))
            .route("/admin/predictions", get(admin_predictions))
            .route("/admin/predictions/update", post(admin_prediction_update))
            .route("/admin/predictions/mode", post(admin_prediction_mode))
            .with_state(self.clone())
            .layer(DefaultBodyLimit::max(self.config.limits.request_bytes))
            .layer(axum::middleware::from_fn_with_state(
                self.clone(),
                http_guard,
            ))
    }
}

async fn readiness(State(gateway): State<Arc<Gateway>>) -> Response {
    if gateway.draining.load(Ordering::Acquire) {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status":"draining"})),
        )
            .into_response()
    } else {
        Json(json!({"status":"ready"})).into_response()
    }
}

fn json_response(status: StatusCode, value: Value) -> Response {
    let bytes = serde_json::to_vec(&value).unwrap_or_default();
    if bytes.is_empty() || bytes.len() > MAX_MESSAGE {
        return (
            StatusCode::BAD_GATEWAY,
            Json(rpc_error(Value::Null, -32002, "response exceeds limit")),
        )
            .into_response();
    }
    (status, [("content-type", "application/json")], bytes).into_response()
}
fn authorize_http(gateway: &Gateway, headers: &HeaderMap) -> Result<Arc<Actor>, StatusCode> {
    if !gateway.origin(headers) {
        return Err(StatusCode::FORBIDDEN);
    }
    gateway
        .authenticate(headers)
        .ok_or(StatusCode::UNAUTHORIZED)
}
fn http_session(
    gateway: &Gateway,
    actor: &Actor,
    headers: &HeaderMap,
) -> Result<Arc<Session>, StatusCode> {
    let id = headers
        .get("MCP-Session-Id")
        .and_then(|value| value.to_str().ok())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let session = gateway.session(actor, id).ok_or(StatusCode::NOT_FOUND)?;
    let supplied = headers
        .get("MCP-Protocol-Version")
        .and_then(|value| value.to_str().ok());
    if supplied.unwrap_or("2025-03-26") != session.version {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(session)
}

async fn http_guard(
    State(gateway): State<Arc<Gateway>>,
    request: Request,
    next: axum::middleware::Next,
) -> Response {
    if request.uri().path() == "/healthz" {
        return next.run(request).await;
    }
    let authorized = if request.uri().path().starts_with("/admin/") {
        admin_authorized(&gateway, request.headers())
    } else {
        gateway.origin(request.headers()) && gateway.authenticate(request.headers()).is_some()
    };
    if !authorized {
        return if !gateway.origin(request.headers()) {
            StatusCode::FORBIDDEN.into_response()
        } else {
            (StatusCode::UNAUTHORIZED, [("WWW-Authenticate", "Bearer")]).into_response()
        };
    }
    let admin = request.uri().path().starts_with("/admin/");
    let readers = if admin {
        gateway.admin_admission.clone()
    } else {
        gateway.body_readers.clone()
    };
    let Ok(read_slot) = readers.try_acquire_owned() else {
        gateway.metrics.rejected.fetch_add(1, Ordering::Relaxed);
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    // Bound every endpoint's body read, including administrative JSON extractors.
    let deadline =
        tokio::time::Instant::now() + Duration::from_millis(gateway.config.request_timeout_ms);
    let (parts, body) = request.into_parts();
    let bytes = match tokio::time::timeout_at(
        deadline.min(tokio::time::Instant::now() + Duration::from_secs(5)),
        to_bytes(body, gateway.config.limits.request_bytes),
    )
    .await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(_)) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        Err(_) => return StatusCode::REQUEST_TIMEOUT.into_response(),
    };
    let byte_slot = if admin {
        None
    } else {
        match gateway
            .body_bytes
            .clone()
            .try_acquire_many_owned(bytes.len() as u32)
        {
            Ok(slot) => Some(slot),
            Err(_) => {
                gateway.metrics.rejected.fetch_add(1, Ordering::Relaxed);
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
        }
    };
    let _admin_slot = if admin {
        Some(read_slot)
    } else {
        drop(read_slot);
        None
    };
    let _byte_slot = byte_slot;
    let mut request = Request::from_parts(parts, Body::from(bytes));
    request.extensions_mut().insert(HttpDeadline(deadline));
    tokio::time::timeout_at(deadline, next.run(request))
        .instrument(tracing::info_span!("gateway.http"))
        .await
        .unwrap_or_else(|_| {
            json_response(
                StatusCode::GATEWAY_TIMEOUT,
                rpc_error(
                    Value::Null,
                    -32008,
                    "HTTP request deadline exceeded; issued effects may have completed",
                ),
            )
        })
}

#[derive(Clone, Copy)]
struct HttpDeadline(tokio::time::Instant);

async fn http_mcp(
    State(gateway): State<Arc<Gateway>>,
    headers: HeaderMap,
    deadline: Option<axum::Extension<HttpDeadline>>,
    body: Body,
) -> Response {
    let actor = match authorize_http(&gateway, &headers) {
        Ok(actor) => actor,
        Err(response) => return response.into_response(),
    };
    let content = headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if content.split(';').next().unwrap_or("").trim() != "application/json" {
        return StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response();
    }
    let accept = headers
        .get("accept")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if !accept.contains("application/json") || !accept.contains("text/event-stream") {
        return StatusCode::NOT_ACCEPTABLE.into_response();
    }
    let bytes =
        match tokio::time::timeout(Duration::from_secs(5), to_bytes(body, MAX_MESSAGE)).await {
            Ok(Ok(bytes)) => bytes,
            _ => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        };
    let value = match wayframe_codec::parse_unique(&bytes) {
        Ok(value) => value,
        Err(_) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                rpc_error(Value::Null, -32700, "invalid JSON"),
            )
        }
    };
    let id = value.get("id").cloned().unwrap_or(Value::Null);
    if !valid_request(&value) {
        return json_response(
            StatusCode::BAD_REQUEST,
            rpc_error(Value::Null, -32600, "invalid JSON-RPC request"),
        );
    }
    if value["method"] == "initialize" {
        if id.is_null() || headers.contains_key("MCP-Session-Id") {
            return json_response(
                StatusCode::BAD_REQUEST,
                rpc_error(id, -32600, "initialize requires a new session"),
            );
        }
        let version = value
            .pointer("/params/protocolVersion")
            .and_then(Value::as_str)
            .filter(|version| VERSIONS.contains(version))
            .unwrap_or(VERSIONS[0]);
        let session = match gateway.create_session(actor, version) {
            Ok(session) => session,
            Err(_) => return StatusCode::TOO_MANY_REQUESTS.into_response(),
        };
        let mut response = json_response(
            StatusCode::OK,
            json!({"jsonrpc":"2.0","id":id,"result":{"protocolVersion":version,"capabilities":{"tools":{}},"serverInfo":{"name":"waypilot","version":env!("CARGO_PKG_VERSION")}}}),
        );
        response.headers_mut().insert(
            "MCP-Session-Id",
            HeaderValue::from_str(&session.id).expect("generated session ID"),
        );
        let predicted = gateway.predictions(&session.actor);
        if !predicted.is_empty() {
            let gateway = gateway.clone();
            tokio::spawn(async move {
                let _ = gateway
                    .prewarm(
                        session,
                        predicted,
                        tokio::time::Instant::now() + Duration::from_secs(5),
                    )
                    .await;
            });
        }
        return response;
    }
    let session = match http_session(&gateway, &actor, &headers) {
        Ok(session) => session,
        Err(response) => return response.into_response(),
    };
    match gateway
        .message(session, value, deadline.map(|value| value.0 .0))
        .await
    {
        Some(reply) => json_response(StatusCode::OK, reply),
        None => StatusCode::ACCEPTED.into_response(),
    }
}

async fn http_get(State(gateway): State<Arc<Gateway>>, headers: HeaderMap) -> Response {
    if let Err(response) = authorize_http(&gateway, &headers) {
        return response.into_response();
    }
    StatusCode::METHOD_NOT_ALLOWED.into_response()
}
async fn http_delete(State(gateway): State<Arc<Gateway>>, headers: HeaderMap) -> Response {
    let actor = match authorize_http(&gateway, &headers) {
        Ok(actor) => actor,
        Err(response) => return response.into_response(),
    };
    let session = match http_session(&gateway, &actor, &headers) {
        Ok(session) => session,
        Err(response) => return response.into_response(),
    };
    session.cancel();
    gateway
        .sessions
        .lock()
        .expect("sessions poisoned")
        .remove(&session.id);
    for client in session.clients.values() {
        client.close().await;
    }
    StatusCode::NO_CONTENT.into_response()
}
async fn http_prewarm(
    State(gateway): State<Arc<Gateway>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let actor = match authorize_http(&gateway, &headers) {
        Ok(actor) => actor,
        Err(response) => return response.into_response(),
    };
    let session = match http_session(&gateway, &actor, &headers) {
        Ok(session) => session,
        Err(response) => return response.into_response(),
    };
    let servers: Vec<String> = match body.get("servers").cloned().map(serde_json::from_value) {
        Some(Ok(servers)) => servers,
        _ => return StatusCode::BAD_REQUEST.into_response(),
    };
    if servers.is_empty() || servers.len() > 2 {
        return StatusCode::BAD_REQUEST.into_response();
    }
    match gateway
        .prewarm(
            session,
            servers,
            tokio::time::Instant::now() + Duration::from_millis(gateway.config.request_timeout_ms),
        )
        .await
    {
        Ok(count) => Json(json!({"prepared":count,"tool_calls":0})).into_response(),
        Err(error) => json_response(
            StatusCode::BAD_GATEWAY,
            rpc_error(Value::Null, error.code, error.message),
        ),
    }
}
fn admin_authorized(gateway: &Gateway, headers: &HeaderMap) -> bool {
    gateway.origin(headers) && gateway.admin(headers)
}
async fn admin_stats(State(gateway): State<Arc<Gateway>>, headers: HeaderMap) -> Response {
    if !admin_authorized(&gateway, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    Json(gateway.snapshot()).into_response()
}
async fn admin_users(State(gateway): State<Arc<Gateway>>, headers: HeaderMap) -> Response {
    if !admin_authorized(&gateway, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    Json(json!({"users":gateway.actors.iter().map(|actor| json!({"id":actor.settings.id,"tenant":actor.settings.tenant,"enabled":actor.enabled.load(Ordering::Acquire),"tools":actor.settings.tools,"speculative_tools":actor.settings.speculative_tools,"requests_per_second":actor.settings.requests_per_second,"burst":actor.settings.burst})).collect::<Vec<_>>()})).into_response()
}
async fn admin_predictions(State(gateway): State<Arc<Gateway>>, headers: HeaderMap) -> Response {
    if !admin_authorized(&gateway, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match &gateway.speculator.learner {
        Some(learner) => {
            Json(learner.report(gateway.epoch.load(Ordering::Acquire))).into_response()
        }
        None => Json(json!({"mode":"disabled","rules":[]})).into_response(),
    }
}
async fn admin_prediction_update(
    State(gateway): State<Arc<Gateway>>,
    headers: HeaderMap,
    Json(value): Json<Value>,
) -> Response {
    if !admin_authorized(&gateway, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let (Some(id), Some(approved)) = (value["id"].as_str(), value["approved"].as_bool()) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(learner) = gateway.speculator.learner.clone() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let id = id.to_owned();
    let mut policy = gateway.policy.clone().lock_owned().await;
    if !learner.has_rule(&id, gateway.epoch.load(Ordering::Acquire)) {
        return StatusCode::NOT_FOUND.into_response();
    }
    match tokio::task::spawn_blocking(move || -> anyhow::Result<Value> {
        policy.prediction(&id, approved)?;
        learner.decide(&id, approved);
        Ok(json!({"id":id,"approved":approved,"persistent":policy.persistent()}))
    })
    .await
    {
        Ok(Ok(value)) => Json(value).into_response(),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
async fn admin_prediction_mode(
    State(gateway): State<Arc<Gateway>>,
    headers: HeaderMap,
    Json(value): Json<Value>,
) -> Response {
    if !admin_authorized(&gateway, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Ok(mode) = serde_json::from_value::<crate::config::LearningMode>(value["mode"].clone())
    else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(learner) = &gateway.speculator.learner else {
        return StatusCode::NOT_FOUND.into_response();
    };
    learner.mode(mode);
    Json(learner.report(gateway.epoch.load(Ordering::Acquire))).into_response()
}
async fn admin_update(
    State(gateway): State<Arc<Gateway>>,
    headers: HeaderMap,
    Json(value): Json<Value>,
) -> Response {
    if !admin_authorized(&gateway, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(id) = value["id"].as_str() else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(enabled) = value["enabled"].as_bool() else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(actor) = gateway.actors.iter().find(|actor| actor.settings.id == id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut policy = gateway.policy.clone().lock_owned().await;
    let actor = actor.clone();
    let id = id.to_owned();
    // Persistence and publication share one owned transaction. Disconnecting the
    // admin caller cannot leave disk and the running authorization table split.
    match tokio::task::spawn_blocking(move || -> anyhow::Result<Value> {
        policy.set(&id, enabled)?;
        actor.enabled.store(enabled, Ordering::Release);
        let generation = gateway.epoch.fetch_add(1, Ordering::AcqRel) + 1;
        gateway.cache.invalidate_generation(generation)?;
        if !enabled {
            let mut sessions = gateway.sessions.lock().expect("sessions poisoned");
            let ids: Vec<_> = sessions.iter().filter(|(_,session)| session.actor.settings.id == id).map(|(id,_)| id.clone()).collect();
            for id in ids { if let Some(session) = sessions.remove(&id) { session.cancel(); } }
        }
        Ok(json!({"id":id,"enabled":enabled,"generation":generation,"persistent":policy.persistent()}))
    }).await {
        Ok(Ok(value)) => Json(value).into_response(),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
async fn admin_invalidate(State(gateway): State<Arc<Gateway>>, headers: HeaderMap) -> Response {
    if !admin_authorized(&gateway, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let generation = gateway.epoch.fetch_add(1, Ordering::AcqRel) + 1;
    match gateway.cache.invalidate_generation(generation) {
        Ok(()) => Json(json!({"generation":generation})).into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
