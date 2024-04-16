use actix_web::web::Data;
use actix_web::{App, HttpServer};

mod api;
mod db;
mod models;

use crate::db::database::DatabaseMSSQL;

use api::mssqlapi::insert_into_hr_employee_table;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    match DatabaseMSSQL::init().await {
        Ok(db) => {
            println!("Database initialized successfully");
            let db_data = Data::new(db);

            HttpServer::new(move || {
                App::new()
                    .app_data(db_data.clone())
                    .service(insert_into_hr_employee_table)
            })
            .bind("127.0.0.1:8080")?
            .run()
            .await
        }
        Err(err) => {
            eprintln!("Error connecting to the database: {}", err);
            std::process::exit(1);
        }
    }
}
