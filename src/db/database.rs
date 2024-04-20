use anyhow::{Error, Result};
use std::env;
use std::sync::{Arc, Mutex};
use tiberius::{Client, Config, Query};

use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

use scraper::{Html, Selector};

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

        pub async fn insert_data_into_hr_employee_table(&self, employee: HREmployees) -> Result<(), tiberius::error::Error> {
            
            let mut client = self.client.lock().expect("Failed to lock client mutex");

            let mut query = Query::new("
                INSERT INTO HR.Employees (lastname, firstname, title, titleofcourtesy, birthdate, hiredate, address, city, region, postalcode, country, phone, mgrid)
                VALUES (@P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10, @P11, @P12, @P13);
            ");
        
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


        pub async fn scrape_currencies_from_narodna_banka(&self) -> Result<(), Box<dyn std::error::Error>> {
            let mut client = self.client.lock().expect("Failed to lock client mutex");
        
            let query_table_make = "
                IF OBJECT_ID('dbo.Currencies', 'U') IS NOT NULL
                    DROP TABLE dbo.Currencies;
                CREATE TABLE dbo.Currencies
                (
                    oznaka_valute VARCHAR(255),
                    sifra_valute INT,
                    naziv_zemlje VARCHAR(500),
                    vazi_za INT,
                    srednji_kurs FLOAT
                );
            ";
        
            let query = Query::new(query_table_make);
            query.execute(&mut client).await?;
        
            // URL of the main page containing the iframe
            let main_url = "https://www.nbs.rs/en/finansijsko_trziste/medjubankarsko-devizno-trziste/kursna-lista/zvanicni-srednji-kurs-dinara/index.html";
        
            // Make an HTTP request to fetch the main page
            let main_req = reqwest::get(main_url).await?;
            let main_html = main_req.text().await?;
        
            // Parse the main page HTML
            let main_doc = Html::parse_document(&main_html);
        
            // Extract the source URL of the iframe
            let iframe_src = main_doc
                .select(&Selector::parse("iframe#frameId").unwrap())
                .next()
                .and_then(|iframe| iframe.value().attr("src"))
                .unwrap_or("");
        
            // Construct the full URL of the iframe
            let iframe_url = format!("https://www.nbs.rs{}", iframe_src);
        
            // Make an HTTP request to fetch the iframe content
            let iframe_req = reqwest::get(&iframe_url).await?;
            let iframe_html = iframe_req.text().await?;
        
            // Parse the iframe HTML
            let iframe_doc = Html::parse_document(&iframe_html);
        
            // Now you can locate and extract the currency data from the iframe
            let currencies = Selector::parse("tr").unwrap();
        
            for (index, currency_row) in iframe_doc.select(&currencies).enumerate() {
                // Skip the first two rows
                if index < 2 {
                    continue;
                }
            
                let td_selector = Selector::parse("td").unwrap();
                let mut columns = currency_row.select(&td_selector).map(|col| col.text().collect::<String>());
            
                // Extract data from columns
                let oznaka_valute = columns.next().unwrap_or_default();
                let sifra_valute = columns.next().unwrap_or_default().parse::<i32>().unwrap_or(0);
                let naziv_zemlje = columns.next().unwrap_or_default();
                let vazi_za = columns.next().unwrap_or_default().parse::<i32>().unwrap_or(0);
                let srednji_kurs_str = columns.next().unwrap_or_default().replace(",", ".");
                let srednji_kurs = match srednji_kurs_str.parse::<f32>() {
                    Ok(val) => val,
                    Err(_) => continue, // Skip this row if srednji_kurs is not a valid float
                };
            
                // Insert data into the table only if all fields are non-empty
                if !oznaka_valute.is_empty() && !naziv_zemlje.is_empty() {
                    // Define the SQL query to insert data into the table
                    let query_insert = "
                        INSERT INTO dbo.Currencies (oznaka_valute, sifra_valute, naziv_zemlje, vazi_za, srednji_kurs)
                        VALUES (@P1, @P2, @P3, @P4, @P5);
                    ";
            
                    // Execute the SQL query to insert data into the table
                    let mut query = Query::new(query_insert);
                    query.bind(oznaka_valute);
                    query.bind(sifra_valute);
                    query.bind(naziv_zemlje);
                    query.bind(vazi_za);
                    query.bind(srednji_kurs);
                    query.execute(&mut client).await?;
                }
            }

            // Execute the stored procedure to update Freight_RSD in Sales.Orders table
            let query_exec = "EXEC CurrecyUpdaterUSDtoRSD;";
            let query = Query::new(query_exec);
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

