use crate::{db::database::DatabaseMSSQL, models::hremployees::HREmployees};
use actix_web::{get, post, web, HttpResponse, Responder};
use validator::Validate;
use anyhow::Error;


//get route for get_currency_data
#[get("/get_currency_data")]
async fn get_currency_data(db: web::Data<DatabaseMSSQL>) -> impl Responder {
   
    match db.get_currency_data().await {
        Ok(curr_data) => {
            if curr_data.is_empty() {
                HttpResponse::NotFound().body("No data available in the database")
            } else {
                HttpResponse::Ok().json(curr_data)
            }
        }
        Err(_) => HttpResponse::InternalServerError().body("Error retrieving Currency data"),
    }
}


//post route for inert data into HR.Employee
#[post("/insert_into_hr_employee_table")]
async fn insert_into_hr_employee_table(db: web::Data<DatabaseMSSQL>, employee: web::Json<HREmployees>) -> impl Responder {
    // Validate the received JSON data
    let employee_data = employee.into_inner();
    let validation_result = employee_data.validate();

    if let Err(validation_errors) = validation_result {
        // Handle validation errors
        let _ = Error::msg(format!("Validation errors: {:?}", validation_errors)); // Logging validation errors
        return HttpResponse::BadRequest().body(format!("Validation errors: {:?}", validation_errors));
    }

    // Call the function to insert data into the HR Employees table
    if let Err(err) = db.insert_data_into_hr_employee_table(employee_data).await {
        // Log the error for debugging purposes
        let _ = Error::msg(format!("Error inserting data: {:?}", err)); // Logging insertion error
        return HttpResponse::InternalServerError().body(format!("Error inserting data: {:?}", err));
    }

    // Return success response
    HttpResponse::Ok().body("Data inserted successfully")
}

//create a api end pint to call scrape_currencies_from_narodna_banka function
#[get("/scrape_currencies_from_narodna_banka_api")]
async fn scrape_currencies_from_narodna_banka_api(db: web::Data<DatabaseMSSQL>) -> impl Responder {
    
    match db.scrape_currencies_from_narodna_banka().await {
        Ok(_) => HttpResponse::Ok().body("Data inserted successfully"),
        Err(err) => HttpResponse::InternalServerError().body(format!("Error inserting data: {:?}", err)),
        
    }
    
}

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
