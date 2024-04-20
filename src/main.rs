use actix_web::{web, App, HttpServer};
use actix_files::Files;
use std::io;
use futures::future;

mod api;
mod db;
mod models;

use crate::db::database::DatabaseMSSQL;

use api::mssqlapi::{insert_into_hr_employee_table, scrape_currencies_from_narodna_banka_api, get_currency_data};

#[actix_web::main]
async fn main() -> io::Result<()> {
    // Initialize the database
    let db = match DatabaseMSSQL::init().await {
        Ok(_) => {
            println!("MSSQL database connection successful!");
            Some(DatabaseMSSQL::init().await.unwrap())
        }
        Err(_) => {
            println!("Failed to initialize MSSQL database");
            None
        }
    };

    let backend_server = if let Some(db) = db {
        HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(db.clone()))
                // .wrap(Logger::default())
                .service(insert_into_hr_employee_table)
                .service(scrape_currencies_from_narodna_banka_api)
                .service(get_currency_data)
        })
        .bind("127.0.0.1:8080")?
        .run()
    } else {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "Failed to start MSSQL server"));
    };

    // Start a separate HTTP server for serving frontend files
    let frontend_server = HttpServer::new(|| {
        App::new().service(Files::new("/", "static").index_file("index.html"))
    })
    .bind("127.0.0.1:3000")?;

    // Run both servers concurrently
    future::try_join(frontend_server.run(), backend_server).await?;

    Ok(())
}
