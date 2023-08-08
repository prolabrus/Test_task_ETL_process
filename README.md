There is a test assignment I received from a potential employer, its original below:

Test task
1.	Create an operational database in any kind of database (whether relational or not)  

 	Options:

  	1.1.	If you do it via MS SQL - you can download adventureworks data sample from microsoft

  	1.2.	Download samples from any source, it is important that there should be at least three tables or more (two dimensions and one fact).

  	1.3.	Three tables:

  	  •	Sales table - sales: customerId, productId, qty
  	
  	  •	Customer table - customers: id, name, country
  	
  	  •	Product table - products: id, name, groupname

  	And insert the data manually.

  	This will be the operational database

3.	Create additional three databases on another instance from the operational database (necessarily relational mysql, postgresql, etc....): mrr, stg, dwh - need to read and understand why we are building 3 databases and what each one is for

4.	In all tables with measures we add the prefix fact in the name, with dimensions dimension + prefix of the database name. Example: if the table in the database is mrr then the table with sales will be mrr_fact_sales, if the table with products in stg then stg_dim_products. In the names only English, and all names of the same format (camel case or snake case). Read what is fact and dimension

5.	Create ETL (airflow, nifi, spark, SSIS or any other ETL) with data transitions from operational database to mrr -> stg -> dwh. 
From the operational database to mrr take data using high water mark(delta). To do this, create a table high_water_mark in which will be the last day or update of each table. In mrr pull in the parameter time according to the table sources and in dwh update high_water_mark with the last value that is in the table.

6.	In each package/dag/process make a system of logs that will be written in the table created for this purpose, the time of execution of the package + if there is an error (this is done in the event handler).

7.	Create a procedure and use there cursor, try and catch (when an error will be written log in the table created for this purpose), make some function.Save all procedures and functions in the dwh database.

8.	Make a dashboard and data model at your discretion in Power BI from the data in dwh.Obligatory with business logic (something simple at your discretion).

9.	Create a script that will do backup for three databases (mrr, stg, dwh).

I've tried to structure it, in a manageable step-by-step way:

1.	Create an operational, relational database. Create 3 tables in it:

  	•	Table sales: customerId, productId, qty
  	
    •	Table customers: id, name, country
  	
    •	Table products: id, name, groupname

  	Insert data into the created tables.

3.	Create additional 3 relational databases on another instance from the operational one: mrr, stg, dwh.

4.	Add prefixes to table names (all names should be of the same format - CamelCase / Snake_case):

  	•	to tables with measures: database name + fact + table name;

  	•	in tables with measurements: database name + dim (dimension) + table name.

  	Examples:

  	•	if the table in the database is mrr, the table with sales will be mrr_fact_sales, 

  	•	if the table with products in stg, then stg_dim_products.
  	
6.	Create ETL (Python) with data transitions from operational database to mrr -> stg -> dwh. Take data from the operational database to mrr using high water mark (delta). To do this, create a table high_water_mark in which will be the last day or update of each table. In mrr pull in the parameter time according to the source table and in dwh update high_water_mark with the last value that is in the table.

7.	In each package/dag/process make a logging system, which will be written in the table created for this purpose, the execution time of the package + if any, the error (this is done in event handler).

8.	Create a procedure and use there cursor, try and catch (in case of error the log will be written to the table created for this purpose) - make some function. Save all procedures and functions in the dwh database.

9.	Make a dashboard and data model at your discretion in Power BI from the data in dwh. Be sure to have business logic (something simple at your discretion).

10.	Create a script that will do backup for three databases (mrr, stg, dwh).

Implementation:
Requires 2 separate instances. I already had MySQL Server installed on the OpenServer Panel, so I downloaded and installed MySQL. Since port 3306 was already busy, I specified port 3307. This way I had 2 independent instances of the same server hosted on different ports.

My idea is to use MySQL to create databases, tables and populate them with data; and Python to create the ETL process. I settled on Python because a variant like Airflow also uses Python to create the ETL process and then load it into the DAG. Since I hadn't programmed in Python before and this experience is new to me (unfortunately HR didn't take this into account), I decided not to complicate the task by adding new variables to the equation. 

So, here is my MySQL code for implementing the test case presented:

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

My Python code to implement an ETL process:

    import mysql.connector
    from datetime import datetime
    
    
    def etl_process():
        # Connection to "the operational" database (memory_db):
        source_connection = mysql.connector.connect(
            host='localhost',
            port=3306,
            user='root',
            password='',
            database='operational_db'
        )
        source_cursor = source_connection.cursor()
        if source_cursor:
            print('host_1 OK')
    
        # Connection to the target (additional database on a separate instance) database ("mrr")
        mrr_connection = mysql.connector.connect(
            host='localhost',
            port=3307,
            user='ruslan',
            password='MySQL@86!',
            database='mrr'
        )
        mrr_cursor = mrr_connection.cursor()
        if mrr_cursor:
            print('host_2 OK')
    
        # Get the current high water mark from the high_water_mark table in the "mrr" database
        mrr_cursor.execute('SELECT MAX(updated) FROM high_water_mark')
        result = mrr_cursor.fetchone()
        if result[0]:
            # If the "high water mark" already exists, use it as a reference point
            last_update = result[0]
        else:
            # If the high water mark is empty, set the start date (e.g. January 1, 2023)
            last_update = datetime(2023,1,1)
    
        # Executing a query to fetch data from "the operational" database using high water mark
        source_cursor.execute('SELECT id, customerId, productId, qty FROM operational_db.sales')
        data_to_transfer = source_cursor.fetchall()
        source_cursor.execute('SELECT id, name, country FROM operational_db.customers')
        customers_data = source_cursor.fetchall()
        source_cursor.execute('SELECT id, name, groupname FROM operational_db.products')
        product_data = source_cursor.fetchall()
    
        error_fact = 0
    
        # Data transfer to "mrr" database
        for row in data_to_transfer:
            id, customerId, productId, qty = row
    
            try:
                # Write the result to the "mrr_table" in the "mrr" database
                mrr_cursor.execute('INSERT INTO mrr.mrr_fact_sales (customerId, productId, qty) VALUES (%s, %s, %s)', (customerId, productId, qty))
                mrr_connection.commit()
    
            except Exception as e:
                # Error handling, if necessary
                error_fact = 1
                # logging errors to a table in the "dwh" database
                mrr_cursor.execute('INSERT INTO dwh.dwh_log (error_time, error_fact) VALUES (%s, %s)', (datetime.now(), error_fact))
    
    
        for row in customers_data:
            id, name, country = row
    
            try:
                mrr_cursor.execute('INSERT INTO mrr.mrr_dim_customers (name, country) VALUES (%s, %s)', (name, country))
                mrr_connection.commit()
    
            except Exception as e:
                # Error handling, if necessary
                error_fact = 1
                # logging errors to a table in the "dwh" database
                mrr_cursor.execute('INSERT INTO dwh.dwh_log (error_time, error_fact) VALUES (%s, %s)', (datetime.now(), error_fact))
    
        for row in product_data:
            id, name, groupname = row
    
            try:
                mrr_cursor.execute('INSERT INTO mrr.mrr_dim_products (name, groupname) VALUES (%s, %s)', (name, groupname))
                mrr_connection.commit()
    
            except Exception as e:
                error_fact = 1
                mrr_cursor.execute('INSERT INTO dwh.dwh_log (error_time, error_fact) VALUES (%s, %s)', (datetime.now(), error_fact))
    
    
        mrr_cursor.execute('SELECT id, customerId, productId, qty FROM mrr.mrr_fact_sales')
        sales_data_2 = mrr_cursor.fetchall()
        mrr_cursor.execute('SELECT id, name, country FROM mrr.mrr_dim_customers')
        customers_data_2 = mrr_cursor.fetchall()
        mrr_cursor.execute('SELECT id, name, groupname FROM mrr.mrr_dim_products')
        product_data_2 = mrr_cursor.fetchall()
    
        # Data transfer to "stg" database
        for row in sales_data_2:
            id, customerId, productId, qty = row
    
            try:
                # Write the result to the table "mrr_table" in the "stg" database
                mrr_cursor.execute('INSERT INTO stg.stg_fact_sales (customerId, productId, qty) VALUES (%s, %s, %s)',
                                   (customerId, productId, qty))
                mrr_connection.commit()
    
            except Exception as e:
                # Error handling, if necessary
                error_fact = 1
                # logging errors to a table in the "dwh" database
                mrr_cursor.execute('INSERT INTO dwh.dwh_log (error_time, error_fact) VALUES (%s, %s)',
                                   (datetime.now(), error_fact))
    
        for row in customers_data_2:
            id, name, country = row
    
            try:
                mrr_cursor.execute('INSERT INTO stg.stg_dim_customers (name, country) VALUES (%s, %s)', (name, country))
                mrr_connection.commit()
    
            except Exception as e:
                # Error handling, if necessary
                error_fact = 1
                # logging errors to a table in the "dwh" database
                mrr_cursor.execute('INSERT INTO dwh.dwh_log (error_time, error_fact) VALUES (%s, %s)',
                                   (datetime.now(), error_fact))
    
        for row in product_data_2:
            id, name, groupname = row
    
            try:
                mrr_cursor.execute('INSERT INTO stg.stg_dim_products (name, groupname) VALUES (%s, %s)', (name, groupname))
                mrr_connection.commit()
    
            except Exception as e:
                error_fact = 1
                mrr_cursor.execute('INSERT INTO dwh.dwh_log (error_time, error_fact) VALUES (%s, %s)',
                                   (datetime.now(), error_fact))
    
    
        mrr_cursor.execute('SELECT customerId, productId, qty FROM stg.stg_fact_sales')
        sales_data_3 = mrr_cursor.fetchall()
        mrr_cursor.execute('SELECT id, name, country FROM stg.stg_dim_customers')
        customers_data_3 = mrr_cursor.fetchall()
        mrr_cursor.execute('SELECT id, name, groupname FROM stg.stg_dim_products')
        product_data_3 = mrr_cursor.fetchall()
    
        # Transfer data to the "dwh" database
        for row in sales_data_3:
            customerId, productId, qty = row
    
            try:
                # Write the result to the table "mrr_table" in the "dwh" database
                mrr_cursor.execute('INSERT INTO dwh.dwh_fact_sales (customerId, productId, qty) VALUES (%s, %s, %s)',
                                   (customerId, productId, qty))
                mrr_connection.commit()
    
            except Exception as e:
                # Error handling, if necessary
                error_fact = 1
                # logging errors to a table in the "dwh" database
                mrr_cursor.execute('INSERT INTO dwh.dwh_log (error_time, error_fact) VALUES (%s, %s)',
                                   (datetime.now(), error_fact))
    
        for row in customers_data_3:
            id, name, country = row
    
            try:
                mrr_cursor.execute('INSERT INTO dwh.dwh_dim_customers (name, country) VALUES (%s, %s)', (name, country))
                mrr_connection.commit()
    
            except Exception as e:
                # Error handling, if necessary
                error_fact = 1
                # logging errors to a table in the "dwh" database
                mrr_cursor.execute('INSERT INTO dwh.dwh_log (error_time, error_fact) VALUES (%s, %s)',
                                   (datetime.now(), error_fact))
    
        for row in product_data_3:
            id, name, groupname = row
    
            try:
                mrr_cursor.execute('INSERT INTO dwh.dwh_dim_products (name, groupname) VALUES (%s, %s)', (name, groupname))
                mrr_connection.commit()
    
            except Exception as e:
                error_fact = 1
                mrr_cursor.execute('INSERT INTO dwh.dwh_log (error_time, error_fact) VALUES (%s, %s)',
                                   (datetime.now(), error_fact))
    
    
        water_list = data_to_transfer
        alone_list = water_list[-1]
        last_id = alone_list[0]
    
        # Updating the "high water mark" in the "mrr" database
        mrr_cursor.execute('INSERT INTO high_water_mark (salesid, updated) VALUES (%s, %s)', (last_id, datetime.now()))
        mrr_connection.commit()
    
        # Closing cursors and connections
        source_cursor.close()
        source_connection.close()
        mrr_cursor.close()
        mrr_connection.close()
    
    etl_process()
