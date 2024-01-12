use std::sync::{Arc, Mutex};

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use dist_scheduler::client::run_client;
use dist_scheduler::Actions;
use futures::{StreamExt, TryStreamExt};
use prost::bytes::Bytes;
use prost::Message;
use tonic::transport::{Endpoint, Uri};
use tonic::{transport::Server, Request, Response, Status};

use futures::stream::BoxStream;
use tonic::Streaming;

use anyhow::anyhow;
use arrow::record_batch::RecordBatch;
use arrow_flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use dashmap::DashMap;
use dist_scheduler::*;

#[derive(Default)]
pub struct TaskScheduler {
    tasks: Mutex<Vec<Task>>,
    // all completed tasks just sink into this map
    _completed_tasks: Arc<DashMap<String, Vec<RecordBatch>>>,
}

#[tonic::async_trait]
impl FlightService for TaskScheduler {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Implement handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Implement list_flights"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        println!("get_flight_info request = {:?}", request);
        Err(Status::unimplemented("Implement get_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Implement get_schema"))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("Implement do_get"))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let data = request.into_inner();
        let record_batch_stream = FlightRecordBatchStream::new_from_flight_data(
            data.map_err(|e| FlightError::DecodeError(e.to_string())),
        );

        // todo, put into completed tasks map

        let out = record_batch_stream.map(|batch| {
            let batch = batch?;
            println!("Received batch = {:?}", batch);
            let mut buf = Vec::new();
            Empty {}.encode(&mut buf).unwrap();

            Ok(arrow_flight::PutResult {
                app_metadata: buf.into(),
            })
        });

        return Ok(Response::new(Box::pin(out)));
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();

        match action.try_into() {
            Ok(Actions::GetTask(_)) => {
                let mut tasks = self.tasks.lock().unwrap();
                let maybe_task = tasks.pop();

                if let Some(task) = maybe_task {
                    // todo, put task back on queue with a pending status
                    let stream = futures::stream::once(async move {
                        let mut buf = Vec::new();
                        task.encode(&mut buf).unwrap();
                        Ok(arrow_flight::Result { body: buf.into() })
                    });

                    Ok(Response::new(Box::pin(stream)))
                } else {
                    let stream = futures::stream::empty();
                    Ok(Response::new(Box::pin(stream)))
                }
            }
            Ok(Actions::SubmitTask(task_req)) => {
                let mut tasks = self.tasks.lock().unwrap();
                let task_id = tasks.len() as u32;
                let task = Task {
                    id: task_id,
                    message: task_req.message,
                    status: Some(TaskStatus::Started(Empty {})),
                };
                tasks.push(task.clone());
                let stream = futures::stream::once(async move {
                    let mut buf = Vec::new();
                    task.encode(&mut buf).unwrap();
                    let body: Bytes = buf.into();

                    Ok(arrow_flight::Result { body })
                });

                Ok(Response::new(Box::pin(stream)))
            }
            Ok(Actions::UpdateTask(task)) => {
                match task.status {
                    Some(TaskStatus::Completed(completed)) => {
                        println!("Task completed = {:?}", completed);
                    }
                    Some(TaskStatus::Started(started)) => {
                        println!("Task started = {:?}", started);
                    }
                    Some(TaskStatus::Running(running)) => {
                        println!("Task running = {:?}", running);
                    }
                    Some(TaskStatus::Failed(failed)) => {
                        println!("Task failed = {:?}", failed);
                    }
                    None => {
                        println!("Task status is None");
                    }
                };

                Ok(Response::new(Box::pin(futures::stream::empty())))
            }
            Err(_) => {
                return Err(Status::invalid_argument("Invalid action type"));
            }
        }
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![
            ActionType {
                r#type: "submit_task".to_string(),
                description: "Submit a task".to_string(),
            },
            ActionType {
                r#type: "get_task".to_string(),
                description: "Get a task".to_string(),
            },
            ActionType {
                r#type: "update_task".to_string(),
                description: "Update a task".to_string(),
            },
        ];
        let stream = futures::stream::iter(actions.into_iter().map(Ok::<_, Status>));

        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Implement do_exchange"))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (client, server) = tokio::io::duplex(1024);
    let task_scheduler = TaskScheduler::default();

    tokio::spawn(async move {
        if let Err(e) = Server::builder()
            .add_service(FlightServiceServer::new(task_scheduler))
            .serve_with_incoming(futures::stream::once(async move {
                Ok::<_, anyhow::Error>(server)
            }))
            .await
        {
            eprintln!("internal error: {}", e);
        }
    });

    let mut client = Some(client);
    let server_channel = Endpoint::try_from("http://[::1]:49552")
        .unwrap()
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let client = client.take();
            async move {
                match client {
                    Some(client) => Ok(client),
                    None => Err(anyhow!("client already taken")),
                }
            }
        }))
        .await
        .map_err(|e| anyhow!("connect with connector: {}", e))?;

    run_client(server_channel).await
}
