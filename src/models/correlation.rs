use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct CorrelationTable {
    pub company_name: String,
    pub max_date_diff_for_shipping: i32,
    pub sales_2022: f64,
    pub sales_2023: f64,
    pub sales_diff: f64,
    pub true_false: i32
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CorrelationStats{
    pub true_count: i32,
    pub false_count: i32,
    pub percent_true: f64,
    pub percent_false: f64
}