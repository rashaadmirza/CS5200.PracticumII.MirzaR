# ----------------------------------------
# BUILD: Practicum II (Summer 2024) / Mine a Database
# Part 2: Create Star/Snowflake Schema
# Rashaad Mohammed Mirza
# mirza.ra@northeastern.edu
# Summer Full 2024
# CS5200 Dr. Martin Schedlbauer
# Northeastern University
# Date: August 6, 2024
# ----------------------------------------

# Load required packages
library(DBI)
library(RSQLite)
library(RMySQL)
library(dplyr)

# ----------------------------------------
# Part 2 / Task 2
# ----------------------------------------

# Connect to SQLite database
sqlite_conn <- dbConnect(RSQLite::SQLite(), dbname = "pharma_sales.db")

# Connect to freemysqlhosting.net MySQL database from R

db_host <- "sql5.freemysqlhosting.net"
db_name <- "sql5724225"
db_user <- "sql5724225"
db_pwd <- "5YeYbiBfvH"
db_port <- 3306

# Connect to remote server database
mysql_conn <-  dbConnect(RMySQL::MySQL(),
                      host = db_host,
                      dbname = db_name,
                      user = db_user,
                      password = db_pwd,
                      port = db_port)

# Check the MySQL version
db_version <- dbGetQuery(mysql_conn, "SELECT VERSION()")
print(db_version)

if(!dbIsValid(mysql_conn)) {
  stop("Connection to MySQL database failed.")
} else {
  print("Connection successful!")
}

# ----------------------------------------
# Part 2 / Task 3
# ----------------------------------------

# --------------------
# Create dimension tables
# --------------------

# Create products_dim table
dbExecute(mysql_conn, "SET FOREIGN_KEY_CHECKS = 0;")
dbExecute(mysql_conn, "DROP TABLE IF EXISTS products_dim")
dbExecute(mysql_conn, "SET FOREIGN_KEY_CHECKS = 1;")
dbExecute(mysql_conn, "
CREATE TABLE products_dim (
  pID INT PRIMARY KEY,
  product VARCHAR(255),
  unitcost DECIMAL(10, 2),
  currency VARCHAR(10)
)")

# Create customers_dim table
dbExecute(mysql_conn, "SET FOREIGN_KEY_CHECKS = 0;")
dbExecute(mysql_conn, "DROP TABLE IF EXISTS customers_dim")
dbExecute(mysql_conn, "SET FOREIGN_KEY_CHECKS = 1;")
dbExecute(mysql_conn, "
CREATE TABLE customers_dim (
  cID INT PRIMARY KEY,
  customer VARCHAR(255),
  country VARCHAR(100)
)")

# Create reps_dim table
dbExecute(mysql_conn, "SET FOREIGN_KEY_CHECKS = 0;")
dbExecute(mysql_conn, "DROP TABLE IF EXISTS reps_dim")
dbExecute(mysql_conn, "SET FOREIGN_KEY_CHECKS = 1;")
dbExecute(mysql_conn, "
CREATE TABLE reps_dim (
  rID INT PRIMARY KEY,
  sur VARCHAR(100),
  first VARCHAR(100),
  phone VARCHAR(50),
  hiredate DATE,
  commission DECIMAL(10, 2),
  territory VARCHAR(50),
  certified BOOLEAN
)")
#$ Note: In MySQL, `certified TINYINT(1) DEFAULT 0`


# Create dates_dim table
dbExecute(mysql_conn, "SET FOREIGN_KEY_CHECKS = 0;")
dbExecute(mysql_conn, "DROP TABLE IF EXISTS dates_dim")
dbExecute(mysql_conn, "SET FOREIGN_KEY_CHECKS = 1;")
dbExecute(mysql_conn, "
CREATE TABLE dates_dim (
  dateID INT PRIMARY KEY AUTO_INCREMENT,
  date DATE,
  year INT,
  quarter INT,
  month INT,
  day INT
)")

#$ Note: Fact tables created in Tasks 3A and 3B below (starting line 175)

# --------------------
# Extract data from SQLite
# --------------------

# Extract products data from SQLite
products_data <- dbGetQuery(sqlite_conn, "SELECT DISTINCT pid AS pID, product, unitcost, currency FROM products")

# Extract customers data from SQLite
customers_data <- dbGetQuery(sqlite_conn, "SELECT DISTINCT cid AS cID, customer, country FROM customers")

# Extract reps data from SQLite
reps_data <- dbGetQuery(sqlite_conn, "SELECT DISTINCT rID, sur, first, phone, hiredate, commission, territory, certified FROM reps")

# Extract unique dates from multiple sales tables
# ----------
# List of sales tables
sales_tables <- c("sales_2020", "sales_2021", "sales_2022", "sales_2023")

# Initialize an empty data frame to store unique dates
all_dates_data <- data.frame(date = as.Date(character()), stringsAsFactors = FALSE)

# Loop through each sales table and extract unique dates
for (table in sales_tables) {
  # Query to extract unique dates
  dates_query <- paste0("SELECT DISTINCT date FROM ", table)
  dates_data <- dbGetQuery(sqlite_conn, dates_query)
  
  # Ensure the date column is of Date type
  dates_data$date <- as.Date(dates_data$date)
  
  # Combine with the main data frame
  all_dates_data <- bind_rows(all_dates_data, dates_data)
}

# Remove duplicates across all tables
all_dates_data <- distinct(all_dates_data)

# Add year, month, day, and quarter columns
all_dates_data <- all_dates_data %>%
  mutate(year = as.numeric(format(as.Date(date), "%Y")),
         month = as.numeric(format(as.Date(date), "%m")),
         day = as.numeric(format(as.Date(date), "%d")),
         quarter = case_when(
           month %in% c(1, 2, 3) ~ 1,
           month %in% c(4, 5, 6) ~ 2,
           month %in% c(7, 8, 9) ~ 3,
           month %in% c(10, 11, 12) ~ 4
         ))
# ----------

# --------------------
# Insert data into MySQL dimension tables
# --------------------

dbWriteTable(mysql_conn, "products_dim", products_data, append = TRUE, row.names = FALSE)
dbWriteTable(mysql_conn, "customers_dim", customers_data, append = TRUE, row.names = FALSE)
dbWriteTable(mysql_conn, "reps_dim", reps_data, append = TRUE, row.names = FALSE)
dbWriteTable(mysql_conn, "dates_dim", all_dates_data, append = TRUE, row.names = FALSE)

# --------------------
# Part 2 / Task 3A / Create and populate the sales_facts table
# --------------------

# Create sales_facts table with references to dimension tables
dbExecute(mysql_conn, "DROP TABLE IF EXISTS sales_facts")
dbExecute(mysql_conn, "
CREATE TABLE sales_facts (
  sales_facts_id INT PRIMARY KEY,
  pID INT,
  dateID INT,
  cID INT,
  rID INT,
  product VARCHAR(255),
  year INT,
  quarter INT,
  month INT,
  country VARCHAR(100),
  territory VARCHAR(50),
  total_amount DECIMAL(10, 2),
  total_units INT,
  FOREIGN KEY (pID) REFERENCES products_dim(pID),
  FOREIGN KEY (dateID) REFERENCES dates_dim(dateID),
  FOREIGN KEY (cID) REFERENCES customers_dim(cID),
  FOREIGN KEY (rID) REFERENCES reps_dim(rID)
)")

# ----------

# We have list of sales tables,
# sales_tables <- c("sales_2020", "sales_2021", "sales_2022", "sales_2023")

# Combine all sales data from SQLite
sales_data <- data.frame()

for (table in sales_tables) {
  query <- paste0("SELECT * FROM ", table)
  temp_data <- dbGetQuery(sqlite_conn, query)
  sales_data <- bind_rows(sales_data, temp_data)
}

# Create a temporary table in MySQL to hold the combined sales data
dbExecute(mysql_conn, "DROP TABLE IF EXISTS combined_sales")
dbExecute(mysql_conn, "
CREATE TABLE combined_sales (
  salesID INT,
  txnID INT,
  rID INT,
  cID INT,
  pID INT,
  date DATE,
  qty INT
)")

# Insert the combined sales data into MySQL
dbWriteTable(mysql_conn, "combined_sales", sales_data, append = TRUE, row.names = FALSE)

# > helps to get `qty`

# ----------

# Query to extract data for sales_facts
sales_facts_query <- "
SELECT
    s.salesID AS sales_facts_id,
    p.pID AS pID,
    d.dateID AS dateID,
    c.cID AS cID,
    r.rID AS rID,
    p.product AS product,
    d.year AS year,
    d.quarter AS quarter,
    d.month AS month,
    c.country AS country,
    r.territory AS territory,
    SUM(s.qty * p.unitcost) AS total_amount,
    SUM(s.qty) AS total_units
FROM combined_sales s
JOIN products_dim p ON s.pID = p.pID
JOIN customers_dim c ON s.cID = c.cID
JOIN reps_dim r ON s.rID = r.rID
JOIN dates_dim d ON DATE(s.date) = d.date
GROUP BY sales_facts_id, pID, cID, dateID, rID, product, year, quarter, month, country, territory
"
# GROUP BY p.pID, c.cID, d.dateID, r.rID, p.product, d.year, d.quarter, d.month, c.country, r.territory

# Execute the query
sales_facts_data <- suppressWarnings(dbGetQuery(mysql_conn, sales_facts_query))

# Insert the retrieved data into sales_facts table in MySQL
dbWriteTable(mysql_conn, "sales_facts", sales_facts_data, append = TRUE, row.names = FALSE)

# --------------------
# Part 2 / Task 3B / Create and populate the rep_facts table
# --------------------

# Create rep_facts table with references to dimension tables
dbExecute(mysql_conn, "DROP TABLE IF EXISTS rep_facts")
dbExecute(mysql_conn, "
CREATE TABLE rep_facts (
    rep_facts_id INT PRIMARY KEY AUTO_INCREMENT,
    rID INT,
    sur VARCHAR(100),
    first VARCHAR(100),
    year INT,
    quarter INT,
    month INT,
    total_amount DECIMAL(10, 2),
    avg_amount DECIMAL(10, 2),
    FOREIGN KEY (rID) REFERENCES reps_dim(rID)
)")

# ----------

# Query to extract data for rep_facts
rep_facts_query <- "
SELECT
    r.rID AS rID,
    r.sur AS sur,
    r.first AS first,
    d.year AS year,
    d.quarter AS quarter,
    d.month AS month,
    SUM(s.qty * p.unitcost) AS total_amount,
    AVG(s.qty * p.unitcost) AS avg_amount
FROM combined_sales s
JOIN reps_dim r ON s.rID = r.rID
JOIN products_dim p ON s.pID = p.pID
JOIN dates_dim d ON DATE(s.date) = d.date
GROUP BY r.rID, r.sur, r.first, d.year, d.quarter, d.month
"

# Execute the query
rep_facts_data <- suppressWarnings(dbGetQuery(mysql_conn, rep_facts_query))

# Insert the retrieved data into rep_facts table in MySQL
dbWriteTable(mysql_conn, "rep_facts", rep_facts_data, append = TRUE, row.names = FALSE)

# ----------------------------------------
# Disconnect from the databases when done
dbDisconnect(sqlite_conn)
dbDisconnect(mysql_conn)

# RStudio Console: >lapply(dbListConnections(RMySQL::MySQL()), dbDisconnect)
# ----------------------------------------

# References:
# https://www.youtube.com/watch?v=H6no78vdNUM
# https://northeastern.instructure.com/courses/180750/modules
# https://www.geeksforgeeks.org/fact-constellation-in-data-warehouse-modelling/
# https://chatgpt.com/
# https://www.databricks.com/glossary/snowflake-schema#:~:text=A%20snowflake%20schema%20is%20a,data%20marts%2C%20and%20relational%20databases.
# https://community.fabric.microsoft.com/t5/Desktop/Same-Dimensions-multiple-Fact-tables-different-measures-millions/td-p/3212805#:~:text=Given%20your%20requirements%2C%20there%20are%20a%20few%20potential%20approaches%20to%20consider.&text=Creating%20a%20star%20schema%20with,a%20bridge%20table%20where%20necessary.
# https://stackoverflow.com/questions/32139596/cannot-allocate-a-new-connection-16-connections-already-opened-rmysql
# https://www.lucidchart.com/pages/tour [ERD]
# https://www.phpmyadmin.co/index.php [Console]