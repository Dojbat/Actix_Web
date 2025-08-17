mod api;
mod repository;
mod model;

use api::task::{
    get_task,
    submit_task,
    start_task,
    fail_task,
    complete_task,
    pause_task
};

use actix_web::{HttpServer, App, web::Data, middleware::Logger};
use repository::ddb::DDBRepository;

#[actix_web::main]
async fn main() -> std::io::Result<()> 
{
    unsafe { std::env::set_var("RUST_LOG", "debug"); }
    unsafe { std::env::set_var("RUST_LOG", "1"); }
    env_logger::init();

    let config = aws_config::load_from_env().await;
    HttpServer::new(move || {
        let ddb_repository: DDBRepository = DDBRepository::init(
            String::from("tasks"), 
            config.clone(),
        );
        let ddb_data = Data::new(ddb_repository);
        let logger = Logger::default();
        App::new()
            .wrap(logger)
            .app_data(ddb_data)
            .service(get_task)
            .service(submit_task)
            .service(start_task)
            .service(complete_task)
            .service(pause_task)
            .service(fail_task)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}