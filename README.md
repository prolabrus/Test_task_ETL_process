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

So, my code for implementing the presented test task consists of 2 parts and can be found in the attached files:
Test_task_ETL_process_-_SQL_part.sql and Test_task_ETL_process_-_Python_part.py.
