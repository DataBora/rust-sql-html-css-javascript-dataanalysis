use anyhow::{Error, Result};
use std::env;
use std::sync::{Arc, Mutex};
use tiberius::{Client, Config};//, AuthMethod
// use tiberius::error::Error as tibError;
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

use crate::models::housing::{YearBuiltCount, AvgSalesPriceByBedroom};

pub struct DatabaseMSSQL {
    pub client: Arc<Mutex<Client<tokio_util::compat::Compat<TcpStream>>>>,
}

// pub struct DatabaseMSSQL {
//     pub client: tiberius::Client<tokio_util::compat::Compat<tokio::net::TcpStream>>,
// }


impl DatabaseMSSQL {

    //-------- INITIALIZING DATABASE --------------//
    pub async fn init() -> Result<Self, Error> {
        dotenv::dotenv().ok();

        let conn_str = env::var("CONNECTION_STRING").expect("CONNECTION_STRING must be set");

        let mut config = Config::from_ado_string(&conn_str)?;

        config.trust_cert();

        // Tokio's TcpStream is used to create a connection
        let tcp = TcpStream::connect(config.get_addr()).await?;
        tcp.set_nodelay(true)?;

        // Use the `Client::connect` method to create a Tiberius client & Make the TcpStream compatible with Tiberius with compat
        let client = Client::connect(config, tcp.compat_write()).await?;
        let client = Arc::new(Mutex::new(client)); // Wrap the client in Arc<Mutex>

        Ok(DatabaseMSSQL{client})
    }


    //-------- CALCULATING COUNT OF TOTAL HOUSES BUILT BY YEAR --------------//
    pub async fn year_built_total(&self) -> Result<Vec<YearBuiltCount>, Error> {

        let mut client = self.client.lock().expect("Failed to lock client mutex");
       
        let mut year_built_counts = Vec::<YearBuiltCount>::new();
    
        if let Some(rows) = 
            client.query("SELECT YearBuilt as year_built, COUNT(YearBuilt) AS total_houses 
                                FROM nashousing 
                                GROUP BY YearBuilt 
                                ORDER BY YearBuilt DESC", &[]).await.ok() {
                
                for row in rows.into_first_result().await? {
                    let year_built: &str = row.get("year_built").unwrap_or_default();
                    let total_houses: i32 = row.get("total_houses").unwrap_or_default();
        
                    // Clean and validate the fields
                    let mut year_built_count = YearBuiltCount {
                        year_built: year_built.to_string(),
                        total_houses,
                    };

                    year_built_count.clean_string_fields();
                    match year_built_count.clean_i32_fields() {
                        Ok(_) => {},
                        Err(err_msg) => {
                            eprintln!("Error cleaning i32 fields: {}", err_msg);
                            continue; // Skip this entry if validation fails
                        }
                    }
        
                    // Print the retrieved data to the console
                    println!("YearBuilt: {}, Total: {}", year_built_count.year_built, year_built_count.total_houses);
        
                    year_built_counts.push(year_built_count);
                }
            } else {
                // Handle the case where client.query returned None
                // You may log an error message or return an appropriate error here
                // depending on your application's requirements
                return Err(Error::msg("Failed to execute SQL query"));
            }
    
        Ok(year_built_counts)
    }

    pub async fn sales_by_bedrooms(&self)-> Result<Vec<AvgSalesPriceByBedroom>, Error>{

        let mut client = self.client.lock().expect("Faled to lock mutes");
        let mut sales_by_bedrooms = Vec::<AvgSalesPriceByBedroom>::new();

        if let Some(rows) = 
            client.query("SELECT YearBuilt as year_built, CONVERT(varchar,SaleDate,23) as sales_date, avg(SalePrice) as avg_sale_price,     Bedrooms as bedrooms FROM nashousing
            WHERE Bedrooms > 6
            GROUP BY YearBuilt, SaleDate, Bedrooms
            ORDER BY Bedrooms DESC;", &[]).await.ok() {
                
                for row in rows.into_first_result().await? {

                    let year_built: &str = row.get("year_built").unwrap_or_default();
                    let sales_date: &str = row.get("sales_date").unwrap_or_default();
                    let avg_sale_price: f64 = row.get("avg_sale_price").unwrap_or_default();
                    let bedrooms: u8 = row.get("bedrooms").unwrap_or_default();
        
                    // Clean and validate the fields
                    let sales_by_bedroom = AvgSalesPriceByBedroom{
                        year_built: year_built.to_string(),
                        sales_date: sales_date.to_string(),
                        avg_sale_price,
                        bedrooms
                    };

                   
        
                    sales_by_bedrooms.push(sales_by_bedroom);
                }
            } else {
                // Handle the case where client.query returned None
                // You may log an error message or return an appropriate error here
                // depending on your application's requirements
                return Err(Error::msg("Failed to execute SQL query"));
            }
    
        Ok(sales_by_bedrooms)


    }
    

    // Function for getting total count for years built
      

        //   // Function to perform some other operation
        //   pub async fn perform_operation(&self, param: &str) -> Result<(), Error> {
        //     // Implement the operation using SQL client
        //     // You can use the same pattern as other functions
        // }

        // Add more methods as needed


}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn year_built_total() {
        // Arrange: Initialize the database connection
        let db = DatabaseMSSQL::init()
            .await
            .expect("Failed to initialize database connection");

        // Act: Call the function you want to test
        let result = db.year_built_total().await;

        // Assert: Check if the result is as expected
        match result {
            Ok(year_built_counts) => {
                if year_built_counts.is_empty() {
                    println!("No data returned from the database.");
                } else {
                    // Print the retrieved data to the console
                    for count in &year_built_counts {
                        println!("YearBuilt: {}, Total: {}", count.year_built, count.total_houses);
                    }
                }
            }
            Err(err) => {
                // Handle the error if the function call fails
                println!("Error occurred: {:?}", err);
                panic!("Error occurred: {:?}", err);
            }
        }
    }

    #[tokio::test]
    async fn sales_by_bedroom() {
        // Arrange: Initialize the database connection
        let db = DatabaseMSSQL::init()
            .await
            .expect("Failed to initialize database connection");

        // Act: Call the function you want to test
        let result = db.sales_by_bedrooms().await;

        // Assert: Check if the result is as expected
        match result {
            Ok(sales) => {
                if sales.is_empty() {
                    println!("No data returned from the database.");
                } else {
                    // Print the retrieved data to the console
                    for count in &sales {
                        println!("YearBuilt: {}, SaleDate: {},AVGSalePrice: {}, Bedrooms: {} ", count.year_built, count.sales_date, count.avg_sale_price,count.bedrooms);
                    }
                }
            }
            Err(err) => {
                // Handle the error if the function call fails
                println!("Error occurred: {:?}", err);
                panic!("Error occurred: {:?}", err);
            }
        }
    }
}

