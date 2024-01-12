use std::sync::Arc;

use crate::Actions;
use crate::{Task, TaskRequest};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::builder::{Int64Builder, StringBuilder};
use arrow_array::{Array, RecordBatch};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::FlightClient;
use futures::TryStreamExt;
use prost::Message;
use reedline::{DefaultPrompt, Reedline, Signal};
use tonic::transport::Channel;

pub async fn run_client(channel: Channel) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = FlightClient::new(channel);

    let mut line_editor = Reedline::create();
    let prompt = DefaultPrompt::default();
    loop {
        let sig = line_editor.read_line(&prompt)?;

        match sig {
            Signal::Success(buffer) => {
                let (cmd, msg) = buffer.split_once(' ').unwrap_or((buffer.as_str(), ""));
                match cmd {
                    "add" => {
                        let task = TaskRequest {
                            message: msg.to_string(),
                        };
                        let action = Actions::submit_task(task).into();
                        let response = client.do_action(action).await?;
                        let res = response.try_collect::<Vec<_>>().await?;
                        for result in res {
                            let _task = Task::decode(result);
                        }
                    }
                    "list" => {
                        todo!()
                    }
                    "g" => {
                        let action = Actions::get_task().into();
                        let response = client.do_action(action).await?;
                        let res = response.try_collect::<Vec<_>>().await?;
                        if res.is_empty() {
                            println!("No tasks available");
                        }
                        for result in res {
                            println!("client result = {:?}", result);
                            let task = Task::decode(result).unwrap();

                            let batch = task_batch(task);
                            let schema = batch.schema();
                            let stream = FlightDataEncoderBuilder::new()
                                .with_schema(schema)
                                .build(futures::stream::once(async move { Ok(batch) }));
                            let res = client.do_put(stream).await?;
                            let _res = res.try_collect::<Vec<_>>().await?;
                        }
                    }
                    "quit" => {
                        break Ok(());
                    }
                    _ => {
                        println!("Unknown command: {}", cmd);
                    }
                }
            }
            Signal::CtrlD | Signal::CtrlC => {
                println!("\nAborted!");
                break Ok(());
            }
        }
    }
}

fn task_batch(task: Task) -> RecordBatch {
    let fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("message", DataType::Utf8, false),
    ];

    let mut id_builder = Int64Builder::new();
    let mut message_builder = StringBuilder::new();
    id_builder.append_value(task.id as i64);
    message_builder.append_value(task.message);
    let id_array: Arc<dyn Array> = Arc::new(id_builder.finish());
    let message_array: Arc<dyn Array> = Arc::new(message_builder.finish());

    let schema = Schema::new(fields);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![id_array, message_array]).unwrap();
    batch
}
