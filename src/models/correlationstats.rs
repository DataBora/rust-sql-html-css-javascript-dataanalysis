use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct CorrelationStats{
    pub true_count: i32,
    pub false_count: i32,
    pub percent_true: f64,
    pub percent_false: f64
}