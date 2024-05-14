use crate::db::database::DatabaseMSSQL;
use actix_web::{get, web, HttpResponse, Responder};


#[get("/get_orders_report")]
async fn get_orders_report(db: web::Data<DatabaseMSSQL>) -> impl Responder {
    match db.sales_orders_report().await {
        Ok(orders_data) => {
            if orders_data.is_empty() {
                HttpResponse::NotFound().body("No data available in the database")
            } else {    
                HttpResponse::Ok().json(orders_data)
            }
        }
        Err(_) => HttpResponse::InternalServerError().body("Error retrieving Orders data"),
    }
}

#[get("/get_customer_sales_by_year")]
async fn get_customer_sales_by_year(db: web::Data<DatabaseMSSQL>) -> impl Responder {
    match db.get_customer_sales_by_year().await {
        Ok(customer_data) => {
            if customer_data.is_empty() {
                HttpResponse::NotFound().body("No data available in the database")
            } else {
                HttpResponse::Ok().json(customer_data)
            }
        }
        Err(_) => HttpResponse::InternalServerError().body("Error retrieving Customer data"),
    }
}

#[get("/get_top_performers")]
async fn get_top_performers(db: web::Data<DatabaseMSSQL>) -> impl Responder {
    match db.get_top_performers().await {
        Ok(top_performers_list) => {
            if top_performers_list.is_empty() {
                HttpResponse::NotFound().body("No data available in the database")
            } else {
                HttpResponse::Ok().json(top_performers_list)
            }
        }
        Err(_) => HttpResponse::InternalServerError().body("Error retrieving Top Performers data"),
    }
}

#[get("/get_sales_choropleth")]
async fn get_sales_choropleth(db: web::Data<DatabaseMSSQL>) -> impl Responder {
    match db.get_sales_choropleth().await {
        Ok(sales_choropleth_list) => {
            if sales_choropleth_list.is_empty() {   
                HttpResponse::NotFound().body("No data available in the database")
            } else {
                HttpResponse::Ok().json(sales_choropleth_list)
            }
        }
        Err(_) => HttpResponse::InternalServerError().body("Error retrieving Sales Choropleth data"),
    }
}






