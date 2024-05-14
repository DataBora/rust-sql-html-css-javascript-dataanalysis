use anyhow::{Error, Result};
use std::env;
use std::sync::{Arc, Mutex};
use tiberius::{Client, Config, Query};

use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

use crate::models::hremployees::HREmployees;
use crate::models::currencydata::Currencies;
use crate::models::ordersreport::OrdersReport;
use crate::models::customerbyyear::CustomerByYear;
use crate::models::topperformers::TopPerformers;
use crate::models::saleschoropleth::SalesChoropleth;
use crate::models::correlation::CorrelationTable;
use crate::models::correlationstats::CorrelationStats;

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

    pub async fn get_correlation_table(&self) -> Result<Vec<CorrelationTable>, Error> {
        let mut client = self.client.lock().expect("Failed to lock client mutex");
        let mut correlation_data = Vec::<CorrelationTable>::new();
        if let Some(rows) = client.query(
            "SELECT sales_by_year.company_name,
                        diff_values.max_date_diff_for_shipping
                        , sales_by_year.sales_2022
                        , sales_by_year.sales_2023
                        , sales_by_year.sales_diff
                        , CASE 
                            WHEN diff_values.max_date_diff_for_shipping > 0 THEN
                                CASE 
                                    WHEN sales_by_year.sales_diff < 0 THEN 1 
                                    ELSE 0 
                                END 
                            ELSE
                                CASE 
                                    WHEN sales_by_year.sales_diff < 0 THEN 1 
                                    ELSE 0 
                                END 
                                    END AS true_false
                        FROM 
                        (SELECT c.companyname as company_name 
                            , CAST(SUM(CASE WHEN YEAR(o.orderdate) = 2022 THEN
                                    od.unitprice * od.qty * (1-od.discount) ELSE 0 END) as FLOAT) as sales_2022
                            , CAST(SUM(CASE WHEN YEAR(o.orderdate) = 2023 THEN
                                    od.unitprice * od.qty * (1 - od.discount) ELSE 0 END) as FLOAT) as sales_2023
                            , (CAST(SUM(CASE WHEN YEAR(o.orderdate) = 2023 THEN
                                    od.unitprice * od.qty * (1-od.discount) ELSE 0 END) as FLOAT)) -
                                (CAST(SUM(CASE WHEN YEAR(o.orderdate) = 2022 THEN
                                    od.unitprice * od.qty * (1 - od.discount) ELSE 0 END) as FLOAT)) as sales_diff
                            FROM Sales.Orders as o
                        JOIN
                            Sales.OrderDetails as od on o.orderid = od.orderid
                        JOIN Sales.Customers as c on o.custid = c.custid
                            GROUP BY c.companyname) as sales_by_year
                        JOIN 
                            (SELECT c.companyname as company_name
                                    , date_diff_custid.max_date_diff_for_shipping
                                FROM Sales.Customers as c
                            JOIN 
                            (SELECT
                                custid
                                , MAX(DATEDIFF(DAY, requireddate, shippeddate)) as max_date_diff_for_shipping
                            FROM Sales.Orders
                            GROUP BY custid) AS date_diff_custid
                                    on c.custid = date_diff_custid.custid) as diff_values on sales_by_year.company_name = diff_values.company_name
                        ORDER BY diff_values.max_date_diff_for_shipping DESC;", &[]).await.ok() {
            for row in rows.into_first_result().await? {
                let company_name: &str = row.get("company_name").expect("Failed to get company_name");
                let max_date_diff_for_shipping: i32 = row.get("max_date_diff_for_shipping").expect("Failed to get max_date_diff_for_shipping");
                let sales_2022: f64 = row.get("sales_2022").expect("Failed to get sales_2022");
                let sales_2023: f64 = row.get("sales_2023").expect("Failed to get sales_2023");
                let sales_diff: f64 = row.get("sales_diff").expect("Failed to get sales_diff");
                let true_false: i32 = row.get("true_false").expect("Failed to get true_false");
                let correlation_table = CorrelationTable {
                    company_name: company_name.to_string(),
                    max_date_diff_for_shipping,
                    sales_2022,
                    sales_2023,
                    sales_diff,
                    true_false
                };
                correlation_data.push(correlation_table);
            }
        } else {
            return Err(Error::msg("Failed to execute SQL query"));
        }
        Ok(correlation_data)
    }

    pub async fn get_correlation_stats_bellow_zero(&self) -> Result<Vec<CorrelationStats>, Error> {
        let mut client = self.client.lock().expect("Failed to lock client mutex");

        let mut correlation_stats = Vec::<CorrelationStats>::new();

        if let Some(rows) = client.query("
                        SELECT  COUNT(CASE WHEN tf_table.true_false = 1 THEN 1 END) as true_count
                            , COUNT(CASE WHEN tf_table.true_false = 0 THEN 0 END) as false_count
                            , CAST(COUNT(CASE WHEN tf_table.true_false = 1 THEN 1 END) * 100.0 / 
                                (COUNT(CASE WHEN tf_table.true_false = 1 THEN 1 END) + COUNT(CASE WHEN tf_table.true_false = 0 THEN 1 END)) AS FLOAT) as percent_true
                            , CAST(COUNT(CASE WHEN tf_table.true_false = 0 THEN 0 END) * 100.0 / 
                                (COUNT(CASE WHEN tf_table.true_false = 1 THEN 1 END) + COUNT(CASE WHEN tf_table.true_false = 0 THEN 1 END)) AS FLOAT)as percent_false
                            FROM
                            (SELECT sales_by_year.company_name,
                                    diff_values.max_date_diff_for_shipping
                                    , sales_by_year.sales_2022
                                    , sales_by_year.sales_2023
                                    , sales_by_year.sales_diff
                                    , CASE 
                                        WHEN diff_values.max_date_diff_for_shipping > 0 THEN
                                            CASE 
                                                WHEN sales_by_year.sales_diff < 0 THEN 1 
                                                ELSE 0 
                                            END 
                                        ELSE
                                            CASE 
                                                WHEN sales_by_year.sales_diff < 0 THEN 1 
                                                ELSE 0 
                                            END 
                                                END AS true_false
                            FROM 
                            (SELECT c.companyname as company_name 
                                , CAST(SUM(CASE WHEN YEAR(o.orderdate) = 2022 THEN
                                        od.unitprice * od.qty * (1-od.discount) ELSE 0 END) as FLOAT) as sales_2022
                                , CAST(SUM(CASE WHEN YEAR(o.orderdate) = 2023 THEN
                                        od.unitprice * od.qty * (1 - od.discount) ELSE 0 END) as FLOAT) as sales_2023
                                , (CAST(SUM(CASE WHEN YEAR(o.orderdate) = 2023 THEN
                                        od.unitprice * od.qty * (1-od.discount) ELSE 0 END) as FLOAT)) -
                                    (CAST(SUM(CASE WHEN YEAR(o.orderdate) = 2022 THEN
                                        od.unitprice * od.qty * (1 - od.discount) ELSE 0 END) as FLOAT)) as sales_diff
                                FROM Sales.Orders as o
                            JOIN
                                Sales.OrderDetails as od on o.orderid = od.orderid
                            JOIN Sales.Customers as c on o.custid = c.custid
                                GROUP BY c.companyname) as sales_by_year
                            JOIN 
                                (SELECT c.companyname as company_name
                                        , date_diff_custid.max_date_diff_for_shipping
                                    FROM Sales.Customers as c
                                JOIN 
                                (SELECT
                                    custid
                                    , MAX(DATEDIFF(DAY, requireddate, shippeddate)) as max_date_diff_for_shipping
                                FROM Sales.Orders
                                GROUP BY custid) AS date_diff_custid
                                        on c.custid = date_diff_custid.custid) as diff_values on sales_by_year.company_name = diff_values.company_name) as tf_table
                          WHERE tf_table.max_date_diff_for_shipping < 0;", &[]).await.ok() {
            for row in rows.into_first_result().await? {
                let true_count: i32 = row.get("true_count").expect("Failed to get true_count");
                let false_count: i32 = row.get("false_count").expect("Failed to get false_count");
                let percent_true: f64 = row.get("percent_true").expect("Failed to get percent_true");
                let percent_false: f64 = row.get("percent_false").expect("Failed to get percent_false");
                let correlation_data = CorrelationStats {
                    true_count,
                    false_count,
                    percent_true,
                    percent_false
                };
                correlation_stats.push(correlation_data);
            }
        } else {
            return Err(Error::msg("Failed to execute SQL query"));
        }
        Ok(correlation_stats)

    }

    pub async fn get_correlation_stats_above_zero(&self) -> Result<Vec<CorrelationStats>, Error> {
        let mut client = self.client.lock().expect("Failed to lock client mutex");

        let mut correlation_stats = Vec::<CorrelationStats>::new();

        if let Some(rows) = client.query("
                    SELECT  COUNT(CASE WHEN tf_table.true_false = 1 THEN 1 END) as true_count
                        , COUNT(CASE WHEN tf_table.true_false = 0 THEN 0 END) as false_count
                        , CAST(COUNT(CASE WHEN tf_table.true_false = 1 THEN 1 END) * 100.0 / 
                            (COUNT(CASE WHEN tf_table.true_false = 1 THEN 1 END) + COUNT(CASE WHEN tf_table.true_false = 0 THEN 1 END)) as FLOAT) as percent_true
                        , CAST(COUNT(CASE WHEN tf_table.true_false = 0 THEN 0 END) * 100.0 / 
                            (COUNT(CASE WHEN tf_table.true_false = 1 THEN 1 END) + COUNT(CASE WHEN tf_table.true_false = 0 THEN 1 END)) as FLOAT) as percent_false
                            FROM
                            (SELECT sales_by_year.company_name,
                                    diff_values.max_date_diff_for_shipping
                                    , sales_by_year.sales_2022
                                    , sales_by_year.sales_2023
                                    , sales_by_year.sales_diff
                                    , CASE 
                                        WHEN diff_values.max_date_diff_for_shipping > 0 THEN
                                            CASE 
                                                WHEN sales_by_year.sales_diff < 0 THEN 1 
                                                ELSE 0 
                                            END 
                                        ELSE
                                            CASE 
                                                WHEN sales_by_year.sales_diff < 0 THEN 1 
                                                ELSE 0 
                                            END 
                                                END AS true_false
                            FROM 
                            (SELECT c.companyname as company_name 
                                , CAST(SUM(CASE WHEN YEAR(o.orderdate) = 2022 THEN
                                        od.unitprice * od.qty * (1-od.discount) ELSE 0 END) as FLOAT) as sales_2022
                                , CAST(SUM(CASE WHEN YEAR(o.orderdate) = 2023 THEN
                                        od.unitprice * od.qty * (1 - od.discount) ELSE 0 END) as FLOAT) as sales_2023
                                , (CAST(SUM(CASE WHEN YEAR(o.orderdate) = 2023 THEN
                                        od.unitprice * od.qty * (1-od.discount) ELSE 0 END) as FLOAT)) -
                                    (CAST(SUM(CASE WHEN YEAR(o.orderdate) = 2022 THEN
                                        od.unitprice * od.qty * (1 - od.discount) ELSE 0 END) as FLOAT)) as sales_diff
                                FROM Sales.Orders as o
                            JOIN
                                Sales.OrderDetails as od on o.orderid = od.orderid
                            JOIN Sales.Customers as c on o.custid = c.custid
                                GROUP BY c.companyname) as sales_by_year
                            JOIN 
                                (SELECT c.companyname as company_name
                                        , date_diff_custid.max_date_diff_for_shipping
                                    FROM Sales.Customers as c
                                JOIN 
                                (SELECT
                                    custid
                                    , MAX(DATEDIFF(DAY, requireddate, shippeddate)) as max_date_diff_for_shipping
                                FROM Sales.Orders
                                GROUP BY custid) AS date_diff_custid
                                        on c.custid = date_diff_custid.custid) as diff_values on sales_by_year.company_name = diff_values.company_name) as tf_table
                            WHERE tf_table.max_date_diff_for_shipping > 0;", &[]).await.ok() {
            for row in rows.into_first_result().await? {
                let true_count: i32 = row.get("true_count").expect("Failed to get true_count");
                let false_count: i32 = row.get("false_count").expect("Failed to get false_count");
                let percent_true: f64 = row.get("percent_true").expect("Failed to get percent_true");
                let percent_false: f64 = row.get("percent_false").expect("Failed to get percent_false");
                let correlation_data = CorrelationStats {
                    true_count,
                    false_count,
                    percent_true,
                    percent_false
                };
                correlation_stats.push(correlation_data);
            }
        } else {
            return Err(Error::msg("Failed to execute SQL query"));
        }
        Ok(correlation_stats)

    }

}
