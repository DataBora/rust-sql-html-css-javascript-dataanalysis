use crate::db::database::DatabaseMSSQL;
use actix_web::{web, HttpResponse, Responder, get};

#[get("/year_built_total_count")]
async fn year_built_total_count(db: web::Data<DatabaseMSSQL>) -> impl Responder {
    // Validate the received JSON data

    match db.year_built_total().await {
        Ok(found_orders) => {
            if found_orders.is_empty() {
                HttpResponse::NotFound().body("No data available in the database")
            } else {
                HttpResponse::Ok().json(found_orders)
            }
        }
        Err(_) => HttpResponse::InternalServerError().body("Error retrieving Year Built Total"),
    }
}

#[get("/sales_by_bedroom")]
async fn sales_by_bedroom(db: web::Data<DatabaseMSSQL>) -> impl Responder {
    // Validate the received JSON data

    match db.sales_by_bedrooms().await {
        Ok(sales) => {
            if sales.is_empty() {
                HttpResponse::NotFound().body("No data available in the database")
            } else {
                HttpResponse::Ok().json(sales)
            }
        }
        Err(_) => HttpResponse::InternalServerError().body("Error retrieving Sales By Bedroom"),
    }
}

