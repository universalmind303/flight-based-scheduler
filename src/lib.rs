pub mod client;
use arrow_flight::{Action, Empty};
use prost::{Message, Oneof};

#[derive(Clone, PartialEq, Message)]
pub struct Task {
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(string, tag = "2")]
    pub message: String,
    #[prost(oneof = "TaskStatus", tags = "8, 9, 10, 11")]
    pub status: Option<TaskStatus>,
}
#[derive(Clone, PartialEq, Message)]
pub struct TaskRequest {
    #[prost(string, tag = "1")]
    pub message: String,
}
#[derive(Clone, PartialEq, Oneof)]
pub enum TaskStatus {
    #[prost(message, tag = "8")]
    Started(Empty),
    #[prost(message, tag = "9")]
    Running(Empty),
    #[prost(message, tag = "10")]
    Completed(Empty),
    #[prost(message, tag = "11")]
    Failed(Empty),
}
pub struct CommandSubmitTask {
    pub task: TaskRequest,
}

pub enum Actions {
    SubmitTask(TaskRequest),
    GetTask(Empty),
    UpdateTask(Task),
}
impl Actions {
    pub fn submit_task(task: TaskRequest) -> Self {
        Actions::SubmitTask(task)
    }
    pub fn get_task() -> Self {
        Actions::GetTask(Empty {})
    }
    pub fn update_task(task: Task) -> Self {
        Actions::UpdateTask(task)
    }
}

impl From<Actions> for Action {
    fn from(action: Actions) -> Self {
        match action {
            Actions::SubmitTask(task) => Action::new("submit_task", task.encode_to_vec()),
            Actions::GetTask(empty) => Action::new("get_task", empty.encode_to_vec()),
            Actions::UpdateTask(task) => Action::new("update_task", task.encode_to_vec()),
        }
    }
}

impl TryFrom<Action> for Actions {
    type Error = prost::DecodeError;
    fn try_from(action: Action) -> Result<Self, Self::Error> {
        match action.r#type.as_str() {
            "submit_task" => Ok(Actions::SubmitTask(TaskRequest::decode(action.body)?)),
            "get_task" => Ok(Actions::GetTask(Empty {})),
            "update_task" => Ok(Actions::UpdateTask(Task::decode(action.body)?)),
            _ => Err(prost::DecodeError::new("Unknown action type")),
        }
    }
}
