//! Authored MCP dataflow regions executed by Wayframe. No trace-inferred dependencies.
use crate::{
    config::Workflow,
    gateway::{Actor, Fault, Gateway, Outcome, Session},
};
use anyhow::Result;
use serde_json::{json, Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};
use wayframe_core::{Context, Decision, Phase, Reason};
use wayframe_flow::{Builder, Error, Flow, Limits, Port};

struct Call {
    gateway: Arc<Gateway>,
    session: Arc<Session>,
    name: String,
    arguments: Value,
    deadline: Instant,
    epoch: u64,
    error: Mutex<Option<Fault>>,
    retained: AtomicUsize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authored_step_names_cannot_collide_with_internal_nodes() {
        let mut config: crate::config::Config =
            toml::from_str(include_str!("../examples/local.toml")).unwrap();
        let workflow = &mut config.workflows[0];
        let names = [
            "inputs",
            "release",
            "a1234567890123456789012345678901",
            "b1234567890123456789012345678901",
        ];
        let mapping: BTreeMap<_, _> = workflow
            .steps
            .iter()
            .zip(names)
            .map(|(step, name)| (step.id.clone(), name.to_string()))
            .collect();
        for step in &mut workflow.steps {
            step.id = mapping[&step.id].clone();
            for binding in step.arguments.values_mut() {
                if let Some(parent) = &mut binding.step {
                    *parent = mapping[parent].clone();
                }
            }
        }
        workflow.outputs = workflow
            .outputs
            .iter()
            .map(|name| mapping[name].clone())
            .collect();
        Compiled::new(workflow.clone(), 4).unwrap();
    }
}
impl Context for Call {
    fn deadline(&self) -> Instant {
        self.deadline
    }
}
struct Inputs {
    call: Arc<Call>,
    values: BTreeMap<String, Value>,
}
#[derive(Clone)]
struct Node {
    result: Value,
    disposition: &'static str,
    elapsed_us: u128,
}

fn fail(call: &Call, fault: Fault) -> Error {
    let mut error = call.error.lock().expect("workflow error poisoned");
    if error.is_none() {
        *error = Some(fault);
    }
    Error::Failed("tool_or_binding_failed")
}
fn gate(call: &Call, required: &[String]) -> Decision {
    if call
        .gateway
        .authorize(&call.session, &call.name, call.epoch)
        .is_err()
        || required.iter().any(|tool| {
            call.gateway
                .authorize(&call.session, tool, call.epoch)
                .is_err()
        })
    {
        return Decision::deny(Reason::Forbidden, "workflow_access_denied");
    }
    Decision::Continue
}
fn limits(parallel: usize) -> Limits {
    Limits {
        max_parallel: parallel.min(4),
        max_executions: 32,
        max_running_nodes: 256,
    }
}
fn failure(call: &Call, error: Error) -> Fault {
    if let Some(error) = call.error.lock().expect("workflow error poisoned").take() {
        return error;
    }
    match error {
        Error::Deadline => Fault {
            code: -32008,
            message: "workflow deadline exceeded; issued effects may have completed",
        },
        Error::Denied(..) => Fault::denied(),
        Error::Overloaded => Fault {
            code: -32029,
            message: "workflow capacity exceeded",
        },
        _ => Fault {
            code: -32002,
            message: "workflow execution failed",
        },
    }
}

pub(crate) struct Single {
    flow: Flow<Call>,
    output: Port<Node>,
}
impl Single {
    pub fn new(maximum: usize) -> Result<Self> {
        let mut builder = Builder::<Call>::new().limits(limits(maximum));
        let root = builder.input();
        let authorized =
            builder.gate("authorize", Phase::Authorize, &root, |call| gate(call, &[]))?;
        let result =
            builder.asynchronous("invoke", Phase::Authorize, &root, |call| async move {
                let result = call
                    .gateway
                    .invoke(
                        call.session.clone(),
                        &call.name,
                        call.arguments.clone(),
                        call.deadline.into(),
                        call.epoch,
                    )
                    .await
                    .map_err(|error| fail(&call, error))?;
                Ok(Node {
                    result: result.result,
                    disposition: result.disposition,
                    elapsed_us: result.elapsed_us,
                })
            })?;
        builder.depends_on(&result, &authorized)?;
        let current = builder.gate("authorize_release", Phase::Authorize, &root, |call| {
            gate(call, &[])
        })?;
        builder.depends_on(&current, &result)?;
        let output =
            builder.dispatch(
                "release",
                &result,
                |result| async move { Ok((*result).clone()) },
            )?;
        builder.depends_on(&output, &authorized)?;
        builder.depends_on(&output, &current)?;
        Ok(Self {
            flow: builder.compile()?,
            output,
        })
    }
    pub async fn run(
        &self,
        gateway: Arc<Gateway>,
        session: Arc<Session>,
        name: String,
        arguments: Value,
        deadline: tokio::time::Instant,
        epoch: u64,
    ) -> Result<Outcome, Fault> {
        let call = Arc::new(Call {
            gateway,
            session,
            name,
            arguments,
            deadline: deadline.into_std(),
            epoch,
            error: Mutex::new(None),
            retained: AtomicUsize::new(0),
        });
        let run = self
            .flow
            .run(call.clone())
            .await
            .map_err(|error| failure(&call, error.error))?;
        let result = run.get(&self.output).expect("released direct result");
        Ok(Outcome {
            result: result.result.clone(),
            disposition: result.disposition,
            elapsed_us: result.elapsed_us,
        })
    }
}

pub(crate) struct Compiled {
    pub definition: Workflow,
    flow: Flow<Call>,
    output: Port<Value>,
}

impl Compiled {
    pub fn new(definition: Workflow, maximum: usize) -> Result<Self> {
        let mut builder = Builder::<Call>::new().limits(limits(maximum));
        let root = builder.input();
        let required: Vec<_> = definition
            .steps
            .iter()
            .map(|step| step.tool.clone())
            .collect();
        let initial_required = required.clone();
        let authorized =
            builder.gate("authorize_workflow", Phase::Authorize, &root, move |call| {
                gate(call, &initial_required)
            })?;
        let base = builder.sync("inputs", Phase::Authorize, &root, |call| {
            Ok(Inputs {
                call,
                values: BTreeMap::new(),
            })
        })?;
        builder.depends_on(&base, &authorized)?;
        let mut ports: BTreeMap<String, Port<Node>> = BTreeMap::new();
        for (index, step) in definition.steps.iter().enumerate() {
            let mut inputs = base.clone();
            let dependencies: BTreeSet<_> = step
                .arguments
                .values()
                .filter_map(|binding| binding.step.clone())
                .collect();
            for (binding_index, dependency) in dependencies.into_iter().enumerate() {
                let name = dependency.clone();
                inputs = builder.join(
                    format!("step-{index}-bind-{binding_index}"),
                    Phase::Authorize,
                    &inputs,
                    ports.get(&dependency).expect("validated dependency"),
                    move |inputs, value| {
                        let mut values = inputs.values.clone();
                        values.insert(name.clone(), value.result.clone());
                        Ok(Inputs {
                            call: inputs.call.clone(),
                            values,
                        })
                    },
                )?;
            }
            let definition = step.clone();
            let port = builder.asynchronous(
                format!("step-{index}"),
                Phase::Authorize,
                &inputs,
                move |inputs| {
                    let step = definition.clone();
                    async move {
                        let mut arguments = Map::new();
                        for (name, binding) in &step.arguments {
                            let source = if let Some(pointer) = &binding.input {
                                inputs.call.arguments.pointer(pointer)
                            } else if let Some(parent) = &binding.step {
                                inputs
                                    .values
                                    .get(parent)
                                    .and_then(|value| value.pointer(&binding.pointer))
                            } else {
                                binding.literal.as_ref()
                            };
                            let Some(value) = source else {
                                return Err(fail(
                                    &inputs.call,
                                    Fault {
                                        code: -32602,
                                        message: "workflow binding did not resolve",
                                    },
                                ));
                            };
                            arguments.insert(name.clone(), value.clone());
                        }
                        let result = inputs
                            .call
                            .gateway
                            .invoke(
                                inputs.call.session.clone(),
                                &step.tool,
                                Value::Object(arguments),
                                inputs.call.deadline.into(),
                                inputs.call.epoch,
                            )
                            .await
                            .map_err(|error| fail(&inputs.call, error))?;
                        if result.result["isError"] == true {
                            return Err(fail(
                                &inputs.call,
                                Fault {
                                    code: -32002,
                                    message: "workflow tool returned an execution error",
                                },
                            ));
                        }
                        let bytes = result.result.to_string().len();
                        if inputs
                            .call
                            .retained
                            .fetch_add(bytes, Ordering::Relaxed)
                            .saturating_add(bytes)
                            > crate::upstream::MAX_MESSAGE
                        {
                            return Err(fail(
                                &inputs.call,
                                Fault {
                                    code: -32002,
                                    message: "workflow retained result budget exceeded",
                                },
                            ));
                        }
                        Ok(Node {
                            result: result.result,
                            disposition: result.disposition,
                            elapsed_us: result.elapsed_us,
                        })
                    }
                },
            )?;
            builder.depends_on(&port, &authorized)?;
            ports.insert(step.id.clone(), port);
        }
        let mut collected = builder.sync("collect-start", Phase::Authorize, &base, |_| {
            Ok(json!({"outputs":{},"steps":[]}))
        })?;
        for (index, (name, port)) in ports.iter().enumerate() {
            let name = name.clone();
            let selected = definition.outputs.contains(&name);
            collected = builder.join(format!("collect-{index}"), Phase::Authorize, &collected, port, move |summary, result| {
                let mut summary = (*summary).clone();
                summary["steps"].as_array_mut().expect("step array").push(json!({"id":name,"disposition":result.disposition,"elapsed_us":result.elapsed_us}));
                if selected { summary["outputs"][&name] = result.result.clone(); }
                Ok(summary)
            })?;
        }
        let current = builder.gate("authorize_release", Phase::Authorize, &root, move |call| {
            gate(call, &required)
        })?;
        builder.depends_on(&current, &collected)?;
        let output = builder.dispatch("release", &collected, |value| async move {
            Ok((*value).clone())
        })?;
        builder.depends_on(&output, &authorized)?;
        builder.depends_on(&output, &current)?;
        Ok(Self {
            definition,
            flow: builder.compile()?,
            output,
        })
    }

    pub fn allowed(&self, actor: &Actor) -> bool {
        self.definition
            .steps
            .iter()
            .all(|step| actor.permits(&step.tool))
    }

    pub async fn run(
        &self,
        gateway: Arc<Gateway>,
        session: Arc<Session>,
        arguments: Value,
        deadline: tokio::time::Instant,
        epoch: u64,
    ) -> Result<Outcome, Fault> {
        // Resolve every input binding before starting any externally visible work.
        if self
            .definition
            .steps
            .iter()
            .flat_map(|step| step.arguments.values())
            .filter_map(|binding| binding.input.as_ref())
            .any(|pointer| arguments.pointer(pointer).is_none())
        {
            return Err(Fault::invalid());
        }
        let started = Instant::now();
        let call = Arc::new(Call {
            gateway,
            session,
            name: format!("waypilot__{}", self.definition.name),
            arguments,
            deadline: deadline.into_std(),
            epoch,
            error: Mutex::new(None),
            retained: AtomicUsize::new(0),
        });
        let run = self
            .flow
            .run(call.clone())
            .await
            .map_err(|error| failure(&call, error.error))?;
        let output = run
            .get(&self.output)
            .expect("released workflow result")
            .clone();
        let result = json!({"content":[{"type":"text","text":output.to_string()}],"structuredContent":output,"isError":false});
        Ok(Outcome {
            result,
            disposition: "workflow",
            elapsed_us: started.elapsed().as_micros(),
        })
    }
}
