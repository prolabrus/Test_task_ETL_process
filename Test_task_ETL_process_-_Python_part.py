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
