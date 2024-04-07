use anyhow::{Error, Result};
use std::env;
use std::sync::{Arc, Mutex};
use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

use crate::models::housing::YearBuiltCount;

pub struct DatabaseMSSQL {
    pub client: Arc<Mutex<Client<tokio_util::compat::Compat<TcpStream>>>>,
}

impl DatabaseMSSQL {

    //-------- INITIALIZING DATABASE --------------//
    pub async fn init() -> Result<Self, Error> {
        dotenv::dotenv().ok();

        let conn_str = env::var("CONNECTION_STRING").expect("CONNECTION_STRING must be set");

        // println!("Connection string: {}", conn_str);

        let mut config = Config::from_ado_string(&conn_str)?;

        config.trust_cert();

        // Tokio's TcpStream is used to create a connection
        let tcp = TcpStream::connect(config.get_addr()).await?;
        tcp.set_nodelay(true)?;

        // Make the TcpStream compatible with Tiberius
        let tcp = tcp.compat_write();

        // Use the `Client::connect` method to create a Tiberius client
        let client = Client::connect(config, tcp).await?;
        let client = Arc::new(Mutex::new(client)); // Wrap the client in Arc<Mutex>

        // println!("Successfully connected to SQL Server");

        Ok(DatabaseMSSQL { client })
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
}

