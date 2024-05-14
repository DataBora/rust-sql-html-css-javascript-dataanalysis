use anyhow::{Error, Result};
use std::env;
use std::sync::{Arc, Mutex};
use tiberius::{Client, Config};

use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

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
