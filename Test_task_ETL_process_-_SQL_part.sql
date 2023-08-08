-- Creation of an operational database:
CREATE DATABASE operational_db;

-- Use of operational database:
USE operational_db;

-- Creating the table "sales" in the operational database:
CREATE TABLE sales (
	id INT PRIMARY KEY AUTO_INCREMENT,
    customerId INT NOT NULL,
    productId INT NOT NULL,
    qty INT NOT NULL
) ENGINE=MEMORY;

-- Creating the table "customers" in the operational database:
CREATE TABLE customers (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    country VARCHAR(100) NOT NULL
) ENGINE=MEMORY;

-- Creating the table "products" in the operational database:
CREATE TABLE products (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    groupname VARCHAR(100) NOT NULL
) ENGINE=MEMORY;

-- Inserting data into the "sales" table of the operational database:
INSERT INTO sales (customerId, productId, qty) VALUES
    (1, 101, 5),
    (2, 102, 3),
    (3, 103, 10),
    (4, 101, 8),
    (5, 103, 2),
    (6, 102, 6),
    (7, 104, 12),
    (8, 101, 4),
    (9, 103, 9),
    (10, 105, 7),
    (11, 102, 1),
    (12, 105, 3),
    (13, 101, 2),
    (14, 104, 5),
    (15, 106, 8),
    (16, 103, 11),
    (17, 101, 6),
    (18, 105, 4),
    (19, 104, 9),
    (20, 102, 7),
    (21, 103, 3),
    (22, 105, 2),
    (23, 101, 1),
    (24, 106, 5),
    (25, 104, 8),
    (26, 103, 6),
    (27, 106, 2),
    (28, 105, 11),
    (29, 102, 9),
    (30, 101, 7);
    
-- Inserting data into the "customers" table of the operational database:
INSERT INTO customers (name, country) VALUES
    ('John Doe', 'USA'),
    ('Jane Smith', 'UK'),
    ('Michael Johnson', 'Canada'),
    ('Emily Brown', 'Australia'),
    ('Andrea Martinez', 'Spain'),
    ('William Lee', 'USA'),
    ('Sophia Kim', 'South Korea'),
    ('Liam Martin', 'Canada'),
    ('Olivia Garcia', 'Spain'),
    ('Noah Rodriguez', 'Mexico'),
    ('Ava Hernandez', 'USA'),
    ('Ethan Lopez', 'Canada'),
    ('Isabella Perez', 'Mexico'),
    ('James Gonzalez', 'USA'),
    ('Mia Martinez', 'Spain'),
    ('Benjamin Wang', 'China'),
    ('Sophia Li', 'USA'),
    ('Alexander Kim', 'South Korea'),
    ('Oliver Chen', 'China'),
    ('Emma Suzuki', 'Japan');

-- Inserting data into the "products" table of the operational database:
INSERT INTO products (name, groupname) VALUES
    ('Product A', 'Group X'),
    ('Product B', 'Group Y'),
    ('Product C', 'Group X'),
    ('Product D', 'Group Z'),
    ('Product E', 'Group Y'),
    ('Product F', 'Group Z'),
    ('Product G', 'Group X'),
    ('Product H', 'Group Y'),
    ('Product I', 'Group Z'),
    ('Product J', 'Group X'),
    ('Product K', 'Group Y'),
    ('Product L', 'Group Z'),
    ('Product M', 'Group X'),
    ('Product N', 'Group Y'),
    ('Product O', 'Group Z'),
    ('Product P', 'Group X'),
    ('Product Q', 'Group Y'),
    ('Product R', 'Group Z'),
    ('Product S', 'Group X'),
    ('Product T', 'Group Y');

-- Create additional "mrr", "stg" and "dwh" databases on a separate instance

-- Creating the "mrr" database:
CREATE DATABASE mrr;

-- Creating a "stg" database:
CREATE DATABASE stg;

-- Creating a "dwh" database:
CREATE DATABASE dwh;

-- Creating empty tables in the "mrr" database

-- Create the "mrr_fact_sales" table in the "mrr" schema:
CREATE TABLE mrr.mrr_fact_sales (
    customerId INT,
    productId INT,
    qty INT
);
-- Creating the "mrr_dim_customers" table in the "mrr" schema:
CREATE TABLE mrr.mrr_dim_customers (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255),
    country VARCHAR(100)
);
-- Creating the table "mrr_dim_products" in the "mrr" schema:
CREATE TABLE mrr.mrr_dim_products (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255),
    groupname VARCHAR(100)
);

-- Logging the update date of each table:
CREATE TABLE high_water_mark (
	id INT PRIMARY KEY AUTO_INCREMENT,
    salesid INT,
	updated DATETIME
    );

-- Creating empty tables in the "stg" database

-- Creating the "stg_fact_sales" table in the "stg" schema:
CREATE TABLE stg.stg_fact_sales (
    customerId INT,
    productId INT,
    qty INT
);
-- Creating the "stg_dim_customers" table in the "stg" schema:
CREATE TABLE stg.stg_dim_customers (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255),
    country VARCHAR(100)
);
-- Creating the "stg_dim_products" table in the "stg" schema
CREATE TABLE stg.stg_dim_products (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255),
    groupname VARCHAR(100)
);


-- Creating empty tables in the "dwh" database

-- Create a "dwh_fact_sales" table in the "dwh" schema:
CREATE TABLE dwh.dwh_fact_sales (
    customerId INT,
    productId INT,
    qty INT
);

-- Creating the "dwh_dim_customers" table in the "dwh" schema:
CREATE TABLE dwh.dwh_dim_customers (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255),
    country VARCHAR(100)
);

-- Creating the "dwh_dim_products" table in the "dwh" schema:
CREATE TABLE dwh.dwh_dim_products (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255),
    groupname VARCHAR(100)
);

-- Create a "dwh_log" table to log errors in the "dwh" circuit:
CREATE TABLE dwh.dwh_log (
    id INT PRIMARY KEY AUTO_INCREMENT,
    error_time DATETIME,
	error_fact INT	
);