use anyhow::{Error, Result};
use std::env;
use std::sync::{Arc, Mutex};
use tiberius::{Client, Config, Query};

use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

use crate::models::hremployees::HREmployees;

pub struct DatabaseMSSQL {
    pub client: Arc<Mutex<Client<tokio_util::compat::Compat<TcpStream>>>>,
}

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
    // pub async fn year_built_total(&self) -> Result<Vec<YearBuiltCount>, Error> {

    //     let mut client = self.client.lock().expect("Failed to lock client mutex");
       
    //     let mut year_built_counts = Vec::<YearBuiltCount>::new();
    
    //     if let Some(rows) = 
    //         client.query("SELECT YearBuilt as year_built, COUNT(YearBuilt) AS total_houses 
    //                             FROM nashousing 
    //                             GROUP BY YearBuilt 
    //                             ORDER BY YearBuilt DESC", &[]).await.ok() {
                
    //             for row in rows.into_first_result().await? {
    //                 let year_built: &str = row.get("year_built").unwrap_or_default();
    //                 let total_houses: i32 = row.get("total_houses").unwrap_or_default();
        
    //                 // Clean and validate the fields
    //                 let mut year_built_count = YearBuiltCount {
    //                     year_built: year_built.to_string(),
    //                     total_houses,
    //                 };

    //                 year_built_count.clean_string_fields();
    //                 match year_built_count.clean_i32_fields() {
    //                     Ok(_) => {},
    //                     Err(err_msg) => {
    //                         eprintln!("Error cleaning i32 fields: {}", err_msg);
    //                         continue; // Skip this entry if validation fails
    //                     }
    //                 }
        
    //                 // Print the retrieved data to the console
    //                 println!("YearBuilt: {}, Total: {}", year_built_count.year_built, year_built_count.total_houses);
        
    //                 year_built_counts.push(year_built_count);
    //             }
    //         } else {
    //             // Handle the case where client.query returned None
    //             // You may log an error message or return an appropriate error here
    //             // depending on your application's requirements
    //             return Err(Error::msg("Failed to execute SQL query"));
    //         }
    
    //     Ok(year_built_counts)
    // }

    
    // Function for getting total count for years built
      

        //   // Function to perform POST methos for inserting data into the database for HR.Employees table
        //   pub async fn perform_operation(&self, param: &str) -> Result<(), Error> {
        //     // Implement the operation using SQL client
        //     // You can use the same pattern as other functions
        // }
        pub async fn insert_data_into_hr_employee_table(&self, employee: HREmployees) -> Result<(), tiberius::error::Error> {
            
            let mut client = self.client.lock().expect("Failed to lock client mutex");

            let mut query = Query::new("
                INSERT INTO HR.Employees (lastname, firstname, title, titleofcourtesy, birthdate, hiredate, address, city, region, postalcode, country, phone, mgrid)
                VALUES (@P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10, @P11, @P12, @P13);
            ");
        
            // let empid = &employee.empid;
            let lastname = &employee.lastname;
            let firstname = &employee.firstname;
            let title = &employee.title;
            let titleofcourtesy = &employee.titleofcourtesy;
            let birthdate = employee.birthdate; 
            let hiredate = employee.hiredate; 
            let address = &employee.address;
            let city = &employee.city;
            let region = &employee.region;
            let postalcode = &employee.postalcode;
            let country = &employee.country;
            let phone = &employee.phone;
            let mgrid = &employee.mgrid;
           
            // query.bind(*empid);
            query.bind(lastname);
            query.bind(firstname);
            query.bind(title);
            query.bind(titleofcourtesy);
            query.bind(birthdate);
            query.bind(hiredate);
            query.bind(address);
            query.bind(city);
            query.bind(region);
            query.bind(postalcode);
            query.bind(country);
            query.bind(phone);
            query.bind(*mgrid);

            query.execute(&mut client).await?; 
        
            Ok(())
        }

       

    }



#[cfg(test)]
mod tests {
    use super::*;

    use chrono::NaiveDate;
    use crate::models::hremployees::{HREmployees, NaiveDateWrapper};

    #[tokio::test]
async fn test_insert_data_into_hr_employee_table() {
    // Arrange: Initialize the database connection
    let db = DatabaseMSSQL::init()
        .await
        .expect("Failed to initialize database connection");

    // Create a sample HREmployees instance for testing
    let employee = HREmployees {
        
        // empid: 10,
        lastname: "Grujicic".to_string(),
        firstname: "Borivoj".to_string(),
        title: "Software Engineer".to_string(),
        titleofcourtesy: "Mr.".to_string(),
        birthdate: NaiveDateWrapper(NaiveDate::from_ymd_opt(1990, 1, 1).unwrap_or_default()),
        hiredate: NaiveDateWrapper(NaiveDate::from_ymd_opt(2020, 1, 1).unwrap_or_default()),
        address: "123 Main St".to_string(),
        city: "Belgrade".to_string(),
        region: "Region".to_string(),
        postalcode: "12345".to_string(),
        country: "SRB".to_string(),
        phone: "123-456-7890".to_string(),
        mgrid: Option::from(3),
    };

    // Act: Call the function you want to test
    let result = db.insert_data_into_hr_employee_table(employee).await;

    // Assert: Check if the result is as expected
    match result {
        Ok(_) => {
            // If the insertion succeeds, print a success message
            println!("Data inserted successfully.");
        }
        Err(err) => {
            // If an error occurs, print the error message
            println!("Error occurred: {:?}", err);
            // Fail the test
            panic!("Error occurred: {:?}", err);
        }
    }
}

}

