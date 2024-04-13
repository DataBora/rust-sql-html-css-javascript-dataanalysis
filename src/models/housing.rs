use serde::{Deserialize,Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct YearBuiltCount {
    pub year_built: String,
    pub total_houses: i32,
}

impl YearBuiltCount {

    pub fn clean_string_fields(&mut self){

        let clean_string_fields = |s: &str| s.chars().filter(|c| c.is_ascii_alphanumeric()).collect::<String>();

        if self.year_built.is_empty()  || self.year_built == "null" {
            self.year_built = String::from("Unknown")
        } else {
            self.year_built = clean_string_fields(&self.year_built)
        }
        
    }


    pub fn clean_i32_fields(&mut self) -> Result<(), &'static str> {

        if self.total_houses == std::i32::MAX {
            self.total_houses = 0;
        } 
        
        else if self.total_houses.is_negative() {
            return Err("Invalid value for total_houses: 0");
        }

        Ok(())
    }
        
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AvgSalesPriceByBedroom {
    pub year_built: String,
    pub sales_date: String,
    pub avg_sale_price: f64,
    pub bedrooms: u8,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AvgPricePerAcreage {
    pub acreage: f64,
    pub avg_price: f64,
}





// pub trait FromRow: Sized{
//     fn from_row(row: Row)-> Result<Self, Error>;
// }

// impl FromRow for YearBuiltCount {
//     fn from_row(row: Row) -> Result<Self, Error> {
//         let year_built: &str = row.try_get(0)?.ok_or(Error::Conversion("Year built is missing".into()))?;
//         let total: i32 = row.try_get(1)?.ok_or(Error::Conversion("Total is missing".into()))?;
//         Ok(YearBuiltCount { year_built: year_built.to_string(), total })
//     }
// }

// impl FromSql<'_> for YearBuiltCount {
//     fn from_sql(column_data: &ColumnData<'_>) -> Result<Option<Self>, Error> {
//         Err(Error::Conversion("Unsupported conversion".into()))
//     }
// }

// impl FromSql<'_> for YearBuiltCount {
//     fn from_sql(column_data: &ColumnData<'_>) -> Result<Option<Self>, Error> {
//         match column_data {
//             ColumnData::String(Some(year_built)) => {
//                 // Convert Cow<str> to String
//                 let year_built = year_built.to_string();
//                 Ok(Some(YearBuiltCount { year_built, total: 0 }))
//             }
//             ColumnData::String(None) => {
//                 // Handle the case where year_built is None
//                 Ok(None)
//             }
//             ColumnData::I32(Some(total)) => {
//                 // Unwrap total and convert to i32
//                 let total = *total;
//                 Ok(Some(YearBuiltCount { year_built: String::new(), total }))
//             }
//             ColumnData::I32(None) => {
//                 // Handle the case where total is None
//                 Ok(None)
//             }
//             _ => Err(Error::Conversion("Unsupported column data type".into())),
//         }
//     }
// }

// impl FromSql<'_> for YearBuiltCount {
//     fn from_sql(column_data: &ColumnData<'_>) -> Result<Option<Self>, Error> {
//         match column_data {
//             ColumnData::String(Some(s)) => {
//                 Ok(Some(YearBuiltCount {
//                     year_built: s.to_string(),
//                     total: 0, // Adjust as needed
//                 }))
//             }
//             ColumnData::String(None) => Ok(None),
//             ColumnData::I32(Some(i)) => {
//                 Ok(Some(YearBuiltCount {
//                     year_built: String::new(), // Adjust as needed
//                     total: *i,
//                 }))
//             }
//             ColumnData::I32(None) => Ok(None),
//             _ => Err(Error::Conversion("Unsupported column data type".into())),
//         }
//     }
// }

// impl YearBuiltCount {
//     pub fn new(year_built: String, total: i32) -> Self {
    //         YearBuiltCount { year_built, total }
    //     }
    // }
