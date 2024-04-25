use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct CustomerByYear {
    pub customer_name: String,
    pub sales_2021: f64,
    pub sales_2022: f64,
    pub sales_2023: f64,
}