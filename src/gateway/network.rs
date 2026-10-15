//! Connection admission and bounded service lifecycle around Axum.
use super::Gateway;
use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Instant, Sleep},
};
use tokio_util::sync::CancellationToken;

pub(super) struct Listener {
    socket: TcpListener,
    slots: Arc<Semaphore>,
    idle: Duration,
    connections: Arc<AtomicU64>,
    stop: CancellationToken,
}
pub(super) struct Connection {
    socket: TcpStream,
    _slot: OwnedSemaphorePermit,
    idle: Duration,
    timer: Pin<Box<Sleep>>,
    stop: Pin<Box<dyn Future<Output = ()> + Send>>,
    connections: Arc<AtomicU64>,
}
impl Drop for Connection {
    fn drop(&mut self) {
        self.connections.fetch_sub(1, Ordering::Relaxed);
    }
}
impl AsyncRead for Connection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.stop.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Ok(()));
        }
        if self.timer.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(io::ErrorKind::TimedOut.into()));
        }
        let before = buffer.filled().len();
        let result = Pin::new(&mut self.socket).poll_read(cx, buffer);
        if matches!(result, Poll::Ready(Ok(()))) && buffer.filled().len() > before {
            let deadline = Instant::now() + self.idle;
            self.timer.as_mut().reset(deadline);
        }
        result
    }
}
impl AsyncWrite for Connection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.stop.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
        }
        Pin::new(&mut self.socket).poll_write(cx, buffer)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.stop.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
        }
        Pin::new(&mut self.socket).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.stop.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}
impl axum::serve::Listener for Listener {
    type Io = Connection;
    type Addr = SocketAddr;
    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.socket.local_addr()
    }
    async fn accept(&mut self) -> (Connection, SocketAddr) {
        loop {
            let slot = self
                .slots
                .clone()
                .acquire_owned()
                .await
                .expect("connection slots stay open");
            match self.socket.accept().await {
                Ok((socket, address)) => {
                    let _ = socket.set_nodelay(true);
                    self.connections.fetch_add(1, Ordering::Relaxed);
                    return (
                        Connection {
                            socket,
                            _slot: slot,
                            idle: self.idle,
                            timer: Box::pin(tokio::time::sleep(self.idle)),
                            stop: Box::pin(self.stop.clone().cancelled_owned()),
                            connections: self.connections.clone(),
                        },
                        address,
                    );
                }
                Err(error) => {
                    tracing::warn!(kind = ?error.kind(), "TCP accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }
}
impl Gateway {
    pub async fn serve_http(
        self: Arc<Self>,
        socket: TcpListener,
        signal: impl Future<Output = ()> + Send + 'static,
    ) -> anyhow::Result<()> {
        let maintenance =
            tokio_util::task::AbortOnDropHandle::new(tokio::spawn(self.clone().maintain()));
        let cleanup = tokio_util::task::AbortOnDropHandle::new(self.cleanup.start());
        let listener = Listener {
            socket,
            slots: Arc::new(Semaphore::new(self.config.limits.max_connections)),
            idle: Duration::from_millis(self.config.limits.connection_idle_ms),
            connections: self.metrics.connections.clone(),
            stop: self.network_stop.clone(),
        };
        let gateway = self.clone();
        let result = axum::serve(listener, self.router())
            .with_graceful_shutdown(async move {
                signal.await;
                maintenance.abort();
                let _ = maintenance.await;
                gateway.shutdown().await;
            })
            .await;
        if !self.draining.load(Ordering::Acquire) {
            self.shutdown().await;
        }
        self.network_stop.cancel();
        cleanup.abort();
        let _ = cleanup.await;
        result.map_err(Into::into)
    }
}
