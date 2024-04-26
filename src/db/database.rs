use anyhow::{Error, Result};
use std::env;
use std::sync::{Arc, Mutex};
use tiberius::{Client, Config, Query};

use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

use scraper::{Html, Selector};

use crate::models::hremployees::HREmployees;
use crate::models::currencydata::Currencies;
use crate::models::ordersreport::OrdersReport;
use crate::models::customerbyyear::CustomerByYear;
use crate::models::topperformers::TopPerformers;
use crate::models::saleschoropleth::SalesChoropleth;

#[derive(Clone)]
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
    pub async fn get_currency_data(&self) -> Result<Vec<Currencies>, Error> {
        let mut client = self.client.lock().expect("Failed to lock client mutex");
       
        let mut currencies_data = Vec::<Currencies>::new();
    
        if let Some(rows) = client.query("SELECT * FROM dbo.Currencies;", &[]).await.ok() {
            for row in rows.into_first_result().await? {
                let oznaka_valute: &str= row.get("oznaka_valute").expect("Failed to get oznaka_valute");
                let sifra_valute: i32 = row.get("sifra_valute").expect("Failed to get sifra_valute");
                let naziv_zemlje: &str = row.get("naziv_zemlje").expect("Failed to get naziv_zemlje");
                let vazi_za: i32 = row.get("vazi_za").expect("Failed to get vazi_za");
                let srednji_kurs: f64 = row.get("srednji_kurs").expect("Failed to get srednji_kurs");
                let datum: &str = row.get("datum").expect("Failed to get datum");
    
                // Clean and validate the fields
                let currency_data = Currencies {
                    oznaka_valute: oznaka_valute.to_string(),
                    sifra_valute,
                    naziv_zemlje: naziv_zemlje.to_string(),
                    vazi_za,
                    srednji_kurs,
                    datum: datum.to_string(),

                };
                currencies_data.push(currency_data);
            }
        } else {
            return Err(Error::msg("Failed to execute SQL query"));
        }
    
        Ok(currencies_data)
    }

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
                    srednji_kurs FLOAT,
                    datum varchar(11)
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

        pub async fn sales_orders_report(&self) -> Result<Vec<OrdersReport>, Error> {

            let mut client = self.client.lock().expect("Failed to lock client mutex");

           
            let mut orders_data = Vec::<OrdersReport>::new();

            if let Some(rows) = client.query(" 
                    SELECT c.companyname as cutomer_name
                        , c.contactname as customer_contact
                        , CASE WHEN c.country = 'UK' then 'United Kingdom'
                                WHEN c.country = 'USA' then 'United States'
                                ELSE c.country
                            END as customer_country
                        , e.lastname + ', ' + e.firstname as employee_name
                        , e.title as employee_title
                        , sh.companyname as shipper_name
                        , o.shipname as ship_name
                        , CAST(o.orderdate as varchar(10)) as order_date
                        , CAST(o.requireddate as varchar(10)) as delivery_date
                        , MAX(o.freight) as freight_value
                        , CAST(SUM (od.unitprice*od.qty*(1-od.discount)) as float) as order_value
                        , CAST(MAX(o.freight) + SUM (od.unitprice*od.qty*(1-od.discount)) as float) as billable_value
                    FROM [Sales].[Orders] as o
                    JOIN [Sales].[Customers] as c on o.custid = c.custid
                    JOIN [HR].[Employees] as e on o.empid = e.empid
                    JOIN [Sales].[Shippers] as sh on o.shipperid = sh.shipperid
                    JOIN [Sales].[OrderDetails] as od on o.orderid = od.orderid
                    JOIN [Production].[Products] as p on od.productid = p.productid
                    GROUP BY c.companyname 
                        , c.contactname 
                        , c.country
                        , e.lastname + ', ' + e.firstname 
                        , e.title 
                        , sh.companyname 
                        , o.shipname
                        , o.orderdate
                        , o.requireddate
            ", &[]).await.ok() {

                for row in rows.into_first_result().await? {

                    let customer_name: &str= row.get("cutomer_name").expect("Failed to get cutomer_name");
                    let customer_contact_name: &str = row.get("customer_contact").expect("Failed to get customer_contact");
                    let customer_country: &str = row.get("customer_country").expect("Failed to get customer_country");
                    let employee_name: &str = row.get("employee_name").expect("Failed to get employee_name");
                    let employee_title: &str = row.get("employee_title").expect("Failed to get employee_title");
                    let shipper_name: &str = row.get("shipper_name").expect("Failed to get shipper_name");
                    let ship_name: &str = row.get("ship_name").expect("Failed to get ship_name");
                    let order_date: &str = row.get("order_date").expect("Failed to get order_date");
                    let delivery_date: &str = row.get("delivery_date").expect("Failed to get delivery_date");
                    let freight_value: f64 = row.get("freight_value").expect("Failed to get freight_value");
                    let order_value: f64 = row.get("order_value").expect("Failed to get order_value");
                    let billable_value: f64 = row.get("billable_value").expect("Failed to get billable_value");

                    let orders_report = OrdersReport {
                        customer_name: customer_name.to_string(),
                        customer_contact_name: customer_contact_name.to_string(),
                        customer_country: customer_country.to_string(),
                        employee_name: employee_name.to_string(),
                        employee_title: employee_title.to_string(),
                        shipper_name: shipper_name.to_string(),
                        ship_name: ship_name.to_string(),
                        order_date: order_date.to_string(),
                        delivery_date: delivery_date.to_string(),
                        freight_value,
                        order_value,
                        billable_value,
                    };
                    orders_data.push(orders_report);

                    // println!("Orders Report: {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}", customer_name, customer_contact_name, customer_country, employee_name, employee_title, shipper_name, ship_name, order_date, delivery_date, freight_value, order_value, billable_value);
                    }
                } else {
                    return Err(Error::msg("Failed to execute SQL query"));
                }

            Ok(orders_data)
            
        }

        pub async fn get_customer_sales_by_year(&self) -> Result<Vec<CustomerByYear>, Error> {
            let mut client = self.client.lock().expect("Failed to lock client mutex");

            let mut customer_data = Vec::<CustomerByYear>::new();

            if let Some(rows) = client.query(
                "SELECT c.companyname as customer_name
                            , CAST(SUM(CASE WHEN YEAR(o.orderdate) = 2021
                                    THEN od.unitprice * od.qty * (1-od.discount) ELSE 0 END) as FLOAT) as [sales_2021]
                            ,CAST(SUM(CASE WHEN YEAR(o.orderdate) = 2022
                                    THEN od.unitprice * od.qty * (1-od.discount) ELSE 0 END) as FLOAT) as [sales_2022]
                            ,CAST(SUM(CASE WHEN YEAR(o.orderdate) = 2023
                                    THEN od.unitprice * od.qty * (1-od.discount) ELSE 0 END) as FLOAT) as [sales_2023]
                        FROM Sales.Orders as o
                        JOIN
                            Sales.OrderDetails as od on o.orderid = od.orderid
                        JOIN Sales.Customers as c on o.custid = c.custid
                        GROUP BY c.companyname
                        ORDER BY 4 DESC", &[]).await.ok() {

                for row in rows.into_first_result().await? {
                    let customer_name: &str= row.get("customer_name").expect("Failed to get customer_name");
                    let sales_2021: f64 = row.get("sales_2021").expect("Failed to get sales_2021");
                    let sales_2022: f64 = row.get("sales_2022").expect("Failed to get sales_2022");
                    let sales_2023: f64 = row.get("sales_2023").expect("Failed to get sales_2023");
                    let customer_by_year = CustomerByYear {
                        customer_name: customer_name.to_string(),
                        sales_2021,
                        sales_2022,
                        sales_2023,
                    };
                    customer_data.push(customer_by_year);
                }
            } else {
                return Err(Error::msg("Failed to execute SQL query"));
            }
            Ok(customer_data)
        }

        pub async fn get_top_performers(&self) -> Result<Vec<TopPerformers>, Error> {
            let mut client = self.client.lock().expect("Failed to lock client mutex");
            let mut top_performers = Vec::<TopPerformers>::new();

            if let Some(rows) = client.query(
                "SELECT 
                            MAX(CASE WHEN companyname = 'Customer THHDP' THEN top_performers ELSE '' END) AS [customer_thhdp],
                            MAX(CASE WHEN companyname = 'Customer CYZTN' THEN top_performers ELSE '' END) AS [customer_cyztn],
                            MAX(CASE WHEN companyname = 'Customer IBVRG' THEN top_performers ELSE '' END) AS [customer_ibvrg],
                            MAX(CASE WHEN companyname = 'Customer FRXZL' THEN top_performers ELSE '' END) AS [customer_frxzl],
                            MAX(CASE WHEN companyname = 'Customer GLLAG' THEN top_performers ELSE '' END) AS [customer_gllag],
                            MAX(CASE WHEN companyname = 'Customer IRRVL' THEN top_performers ELSE '' END) AS [customer_irrvl],
                            MAX(CASE WHEN companyname = 'Customer NYUHS' THEN top_performers ELSE '' END) AS [customer_nyuhs],
                            MAX(CASE WHEN companyname = 'Customer LCOUJ' THEN top_performers ELSE '' END) AS [customer_lcouj],
                            MAX(CASE WHEN companyname = 'Customer SFOGW' THEN top_performers ELSE '' END) AS [customer_sfogw],
                            MAX(CASE WHEN companyname = 'Customer YBQTI' THEN top_performers ELSE '' END) AS [customer_ybqti]
                        FROM (
                            SELECT 
                                c.companyname, 
                                top_performers,
                                ROW_NUMBER() OVER (PARTITION BY c.companyname ORDER BY top_performers) AS RowNum
                            FROM 
                                Sales.Customers AS c 
                            JOIN (
                                SELECT DISTINCT
                                    e.lastname + ', ' +  e.firstname AS top_performers, 
                                    o.custid  
                                FROM 
                                    HR.Employees AS e
                                JOIN
                                    Sales.Orders AS o ON e.empid = o.empid
                                JOIN (
                                    SELECT TOP 10 WITH TIES 
                                        o.custid,
                                        c.companyname AS customer_name,
                                        SUM(CASE WHEN YEAR(o.orderdate) = 2023 
                                            THEN od.unitprice * od.qty * (1 - od.discount) ELSE 0 END) AS [sales_2023]
                                    FROM 
                                        Sales.Orders AS o
                                    JOIN 
                                        Sales.OrderDetails AS od ON o.orderid = od.orderid
                                    JOIN
                                        Sales.Customers AS c ON o.custid = c.custid
                                    GROUP BY 
                                        o.custid, c.companyname
                                    ORDER BY 
                                        sales_2023 DESC
                                ) AS top_10_customers_2023 ON top_10_customers_2023.custid = o.custid
                            ) AS performers_custid ON performers_custid.custid = c.custid
                        ) AS top_company_employee
                        GROUP BY RowNum;", &[]).await.ok() {

                for row in rows.into_first_result().await? {
                    let customer_thhdp: &str = row.get("customer_thhdp").expect("Failed to get customer_thhdp");
                    let customer_cyztn: &str = row.get("customer_cyztn").expect("Failed to get customer_cyztn");
                    let customer_ibvrg: &str = row.get("customer_ibvrg").expect("Failed to get customer_ibvrg");
                    let customer_frxzl: &str = row.get("customer_frxzl").expect("Failed to get customer_frxzl");
                    let customer_gllag: &str = row.get("customer_gllag").expect("Failed to get customer_gllag");
                    let customer_irrvl: &str = row.get("customer_irrvl").expect("Failed to get customer_irrvl");
                    let customer_nyuhs: &str = row.get("customer_nyuhs").expect("Failed to get customer_nyuhs");
                    let customer_lcouj: &str = row.get("customer_lcouj").expect("Failed to get customer_lcouj");
                    let customer_sfogw: &str = row.get("customer_sfogw").expect("Failed to get customer_sfogw");
                    let customer_ybqti: &str = row.get("customer_ybqti").expect("Failed to get customer_ybqti");

                    let top_performer = TopPerformers {
                        customer_thhdp: customer_thhdp.to_string(),
                        customer_cyztn: customer_cyztn.to_string(),
                        customer_ibvrg: customer_ibvrg.to_string(),
                        customer_frxzl: customer_frxzl.to_string(),
                        customer_gllag: customer_gllag.to_string(),
                        customer_irrvl: customer_irrvl.to_string(),
                        customer_nyuhs: customer_nyuhs.to_string(),
                        customer_lcouj: customer_lcouj.to_string(),
                        customer_sfogw: customer_sfogw.to_string(),
                        customer_ybqti: customer_ybqti.to_string(),
                    };
                    top_performers.push(top_performer);
                }
            }
            Ok(top_performers)    
    }  

    pub async fn get_sales_choropleth(&self) -> Result<Vec<SalesChoropleth>, Error> {
        let mut client = self.client.lock().expect("Failed to lock client mutex");

        let mut sales_choropleth_data = Vec::<SalesChoropleth>::new();

        if let Some(rows) = client.query(
            "SELECT c.country AS country
                    , CAST(SUM(CASE WHEN YEAR(o.orderdate) = 2023
                            THEN od.unitprice * od.qty * (1-od.discount) ELSE 0 END) as FLOAT) as [sales_2023]
                    FROM Sales.Orders as o
                    JOIN
                        Sales.OrderDetails as od on o.orderid = od.orderid
                    JOIN Sales.Customers as c on o.custid = c.custid
                    GROUP BY c.country
                    ORDER BY 2 desc;", &[]).await.ok() {
            for row in rows.into_first_result().await? {

                let country: &str = row.get("country").expect("Failed to get country");
                let sales_2023: f64 = row.get("sales_2023").expect("Failed to get sales_2023");
                
                let sales_choropleth = SalesChoropleth {
                    country: country.to_string(),
                    sales_2023
                };
                sales_choropleth_data.push(sales_choropleth);
            }
        } else {
            return Err(Error::msg("Failed to execute SQL query"));
        }
        Ok(sales_choropleth_data)    
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

