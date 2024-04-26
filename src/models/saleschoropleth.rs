use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct SalesChoropleth {
    pub country: String,
    pub sales_2023: f64
}