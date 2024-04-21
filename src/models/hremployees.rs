use serde::{Deserialize,Serialize, Serializer, Deserializer};
use validator::{Validate, ValidationError};
use chrono::NaiveDate;
use tiberius::{ColumnData, IntoSql};


#[derive(Serialize, Deserialize, Debug, Validate)]
pub struct HREmployees {
    #[validate(length(min = 1, max = 20), custom(function=string_ascii_check))]
    pub lastname: String,
    #[validate(length(min = 1, max = 10), custom(function=string_ascii_check))]
    pub firstname: String,
    #[validate(length(min = 1, max = 50))]
    pub title: String,
    #[validate(length(min = 1, max = 30))]
    pub titleofcourtesy: String,
    pub birthdate: NaiveDateWrapper, 
    pub hiredate: NaiveDateWrapper,
    #[validate(length(min = 1, max = 50))]
    pub address: String,
    #[validate(length(min = 1, max = 60), custom(function=string_ascii_check))]
    pub city: String,
    #[validate(length(min = 1, max = 50), custom(function=string_ascii_check))]
    pub region: String,
    #[validate(length(min = 1, max = 10), custom(function=string_ascii_check))]
    pub postalcode: String,
    #[validate(length(min = 1, max = 15), custom(function=string_ascii_check))]
    pub country: String,
    #[validate(length(min = 1, max = 24))]
    pub phone: String,
    pub mgrid: Option<i32>,
}

fn string_ascii_check(s: &str) -> Result<(), ValidationError> {
    if !s.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err(ValidationError::new("Invalid characters in string"));
    }
    Ok(())
}

// impl HREmployees{
//     pub fn clean_string_ascii_check(s: &str) -> Result<(), ValidationError> {
//         if !s.chars().all(|c| c.is_ascii_alphanumeric()) {
//             return Err(ValidationError::new("Invalid characters in string"));
//         } else {
//             Ok(())
//         }
//     }
    
// }


#[derive(Debug)]
pub struct NaiveDateWrapper(pub NaiveDate);

impl Serialize for NaiveDateWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for NaiveDateWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let date = NaiveDate::parse_from_str(&s, "%Y-%m-%d").map_err(serde::de::Error::custom)?;
        Ok(NaiveDateWrapper(date))
    }
}

impl IntoSql<'_> for NaiveDateWrapper {
    fn into_sql(self) -> ColumnData<'static> {
        ColumnData::String(Some(self.0.to_string().into()))
    }
}

