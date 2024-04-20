use serde::{Deserialize,Serialize};
use validator::Validate;



#[derive(Serialize, Deserialize, Debug, Validate)]
pub struct Currencies {
    #[validate(length(min = 1, max = 20))]
    pub oznaka_valute: String,
    pub sifra_valute: i32,
    #[validate(length(min = 1, max = 50))]
    pub naziv_zemlje: String,
    pub vazi_za: i32,
    pub srednji_kurs: f64,
}