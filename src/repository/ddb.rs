use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::model::AttributeValue;
use aws_config::Config;
use crate::model::task::{Task, TaskState};
use log::error;
use std::str::FromStr;
use std::collections::HashMap;

pub struct DDBRepository {
    client: Client,
    table_name: String
}

#[derive(Debug)]
pub struct DDBError;

/// Helper function to extract a required item value from a DynamoDB item
fn required_item_value(key: &str, item: &HashMap<String, AttributeValue>) -> Result<String, DDBError> {
    match item_value(key, item) {
        Ok(Some(value)) => Ok(value),
        Ok(None) => Err(DDBError),
        Err(DDBError) => Err(DDBError)
    }
}
/// Helper function to extract an optional item value from a DynamoDB item
fn item_value(key: &str, item: &HashMap<String, AttributeValue>) -> Result<Option<String>, DDBError> {
    match item.get(key) {
        Some(value) => match value.as_s() {
            Ok(val) => Ok(Some(val.clone())),
            Err(_) => Err(DDBError)
        },
        None => Ok(None)
    }
}

/// Helper function to convert a DynamoDB item to a Task
fn item_to_task(item: &HashMap<String, AttributeValue>) -> Result<Task, DDBError> {
    let state: TaskState = match TaskState::from_str(required_item_value("state", item)?.as_str()) {
        Ok(value) => value,
        Err(_) => return Err(DDBError)
    };

    let result_file = item_value("result_file", item)?;

    Ok(Task {
        user_uuid: required_item_value("user_uuid", item)?,
        task_uuid: required_item_value("task_uuid", item)?,
        task_type: required_item_value("task_type", item)?,
        state,
        source_file: required_item_value("source_file", item)?,
        result_file
    })
}

impl DDBRepository {
    /// Initialize a new DDBRepository
    pub fn init(table_name: String, config: Config) -> DDBRepository {
        let client = Client::new(&config);
        DDBRepository {
            table_name,
            client
        }
    }

    /// Put a task into the DynamoDB table
    pub async fn put_task(&self, task: Task) -> Result<(), DDBError> {
        dbg!(&task);
        let mut request = self.client.put_item()
                .table_name(&self.table_name)
                .item("task_global_id", AttributeValue::S(task.get_global_id()))
                .item("user_uuid", AttributeValue::S(task.user_uuid.clone()))
                .item("task_uuid", AttributeValue::S(task.task_uuid.clone()))
                .item("task_type", AttributeValue::S(task.task_type.clone()))
                .item("state", AttributeValue::S(task.state.to_string()))
                .item("source_file", AttributeValue::S(task.source_file.clone()));
        
        if let Some(ref result_file) = task.result_file {
                request = request.item("result_file", AttributeValue::S(result_file.to_string()));
        }

        match request.send().await {
            Ok(_) => {
                println!("DynamoDB put_item succeeded for {}", task.get_global_id());
                Ok(())
            },
            Err(e) => {
                eprintln!("DynamoDB put_item error: {:#?}", e);
                Err(DDBError)
            }
        }
    }

    /// Get a task from the DynamoDB table
    pub async fn get_task(&self, task_id: String) -> Option<Task> {
        println!("get_task: fetching task_id = {}", task_id);
        let res = self.client
            .get_item()
            .table_name(&self.table_name)
            .key("task_global_id", AttributeValue::S(task_id.clone()))
            .send()
            .await;

        return match res {
            Ok(output) => {
                println!("get_task: raw DynamoDB output = {:?}", output);
                match output.item {
                    Some(item) => {
                        println!("get_task: raw item = {:?}", &item);
                        match item_to_task(&item) {
                            Ok(task) => Some(task),
                            Err(_) => None
                        }
                    },
                    None => None
                }
            },
            Err(error) => {
                println!("get_task: DynamoDB error = {:?}", error);
                None
            }
        }
    }
}