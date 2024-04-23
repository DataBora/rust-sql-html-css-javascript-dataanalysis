use serde::{Deserialize,Serialize};


#[derive(Serialize, Deserialize, Debug)]
pub struct OrdersReport {
    pub customer_name: String,
    pub customer_contact_name: String,
    pub customer_country: String,
    pub employee_name: String,
    pub employee_title: String,
    pub shipper_name: String,
    pub ship_name: String,
    pub order_date: String,
    pub delivery_date: String,
    pub freight_value: f64,
    pub order_value: f64,
    pub billable_value: f64,
}