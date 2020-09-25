import csv
import mysql.connector
from mysql.connector import Error
import os.path as path
import datetime

path_dir = r"C:\Assignment\Data-CaseStudy"
source_csv = r"sales_data_sample.csv"

host_name="127.0.0.1"
db_name = "salesdwh"
my_username = "raouf"
my_password = "123456"

def test_database_connection(host, db, username, password):
    try:
        connection = mysql.connector.connect(host= host,
                                             database= db,
                                             user= username,
                                             password= password,
                                             auth_plugin='mysql_native_password')
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)
    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

def populate_geo_dimension(conn, row):
    territory_name = row["TERRITORY"]
    if territory_name.lower() == "japan":
        territory_name = "APAC"
    country_name = row["COUNTRY"]
    state_name = row["STATE"]
    if state_name == "":
        state_name = "N/A"
    city_name = row["CITY"]

    mycursor = conn.cursor()
    q_select_territory_id = """SELECT TerritoryID FROM territory WHERE Name = %s"""
    param = (territory_name, )
    mycursor.execute(q_select_territory_id, param)
    result_select_territory_id = mycursor.fetchall()
    if len(result_select_territory_id) == 0:
        q_insert_territory = "INSERT INTO territory (Name) VALUES (%s)"
        mycursor.execute(q_insert_territory, param)
        conn.commit()
        territory_id = mycursor.lastrowid
    elif len(result_select_territory_id) == 1:
        territory_id = result_select_territory_id[0][0]
    else:
        raise Exception("Duplicate TerritoryID")

    q_select_country_id = """SELECT CountryID FROM country WHERE Name = %s"""
    param = (country_name,)
    mycursor.execute(q_select_country_id, param)
    result_select_country_id = mycursor.fetchall()
    if len(result_select_country_id) == 0:
        q_insert_country = """INSERT INTO country (Name, TerritoryID) VALUES (%s, %s)"""
        param = (country_name, territory_id, )
        mycursor.execute(q_insert_country, param)
        conn.commit()
        country_id = mycursor.lastrowid
    elif len(result_select_country_id) == 1:
        country_id = result_select_country_id[0][0]
    else:
        raise Exception("Duplicate CountryID")

    q_select_state_id = """SELECT StateID FROM state WHERE Name = %s AND CountryID = %s"""
    param = (state_name, country_id)
    mycursor.execute(q_select_state_id, param)
    result_select_state_id = mycursor.fetchall()
    if len(result_select_state_id) == 0:
        q_insert_state = """INSERT INTO state (Name, CountryID) VALUES (%s, %s)"""
        param = (state_name, country_id,)
        mycursor.execute(q_insert_state, param)
        conn.commit()
        state_id = mycursor.lastrowid
    elif len(result_select_state_id) == 1:
        state_id = result_select_state_id[0][0]
    else:
        raise Exception("Duplicate StateID")

    q_select_city_id = """SELECT CityID FROM city 
                            WHERE Name = %s 
                            AND StateID = %s"""
    param = (city_name, state_id)
    mycursor.execute(q_select_city_id, param)
    result_select_city_id = mycursor.fetchall()
    if len(result_select_city_id) == 0:
        q_insert_city = """INSERT INTO city (Name, StateID) VALUES (%s, %s)"""
        param = (city_name, state_id)
        mycursor.execute(q_insert_city, param)
        conn.commit()
        city_id = mycursor.lastrowid
    elif len(result_select_city_id) == 1:
        city_id = result_select_state_id[0][0]
    else:
        raise Exception("Duplicate CityID")

    mycursor.close()
    return city_id


def populate_customer_dimension(conn, row):
    customer_name = row["CUSTOMERNAME"]
    postal_code = row["POSTALCODE"]
    if postal_code == "" or  len(postal_code) == 0:
        postal_code == "N/A"
    print("***"+postal_code+"***")
    city_name = row["CITY"]
    rep_name = row["CONTACTFIRSTNAME"] + " " +row["CONTACTLASTNAME"]

    mycursor = conn.cursor()
    q_select_customer_id = """SELECT CustomerID FROM customer
                                WHERE Name = %s 
                                AND RepName = %s 
                                AND CityName = %s"""
    param = (customer_name, rep_name, city_name)
    mycursor.execute(q_select_customer_id, param)
    result_select_customer_id = mycursor.fetchall()
    if len(result_select_customer_id) == 0:
        q_insert_customer = """INSERT INTO Customer 
                                (Name, PostalCode, RepName, CityName) 
                                VALUES (%s, %s, %s, %s)"""
        param = (customer_name, postal_code, rep_name, city_name)
        mycursor.execute(q_insert_customer, param)
        conn.commit()
        customer_id = mycursor.lastrowid
    elif len(result_select_customer_id) == 1:
        customer_id = result_select_customer_id[0][0]
    else:
        raise Exception("Duplicate CustomerID")

    mycursor.close()
    return customer_id

def populate_product_dimension(conn, row):
    product_line_name = row["PRODUCTLINE"]
    product_code = row["PRODUCTCODE"]
    product_msrp = row["MSRP"]

    mycursor = conn.cursor()
    q_select_product_line_id = """SELECT ProductLineID FROM ProductLine WHERE Name = %s """
    param = (product_line_name,)
    mycursor.execute(q_select_product_line_id, param)
    result_select_product_line_id = mycursor.fetchall()
    if len(result_select_product_line_id) == 0:
        q_insert_product_line = "INSERT INTO ProductLine (Name) VALUES (%s)"
        param = (product_line_name,)
        mycursor.execute(q_insert_product_line, param)
        conn.commit()
        product_line_id = mycursor.lastrowid
    elif len(result_select_product_line_id) == 1:
        product_line_id = result_select_product_line_id[0][0]
    else:
        raise Exception("Duplicate ProductLineID")

    q_select_product_id = """SELECT ProductID FROM Product WHERE ProductID = %s """
    param = (product_code,)
    mycursor.execute(q_select_product_id, param)
    result_select_product_id = mycursor.fetchall()
    if len(result_select_product_id) == 0:
        q_insert_product = "INSERT INTO Product (ProductID, ProductLineID, MSRP) VALUES (%s, %s, %s)"
        param = (product_code, product_line_id, product_msrp)
        mycursor.execute(q_insert_product, param)
        conn.commit()
        product_id = product_code
    elif len(result_select_product_id) == 1:
        product_id = result_select_product_id[0][0]
    else:
        raise Exception("Duplicate ProductID")

    return product_id

def populate_time_dimension(conn, row):
    order_date = row["ORDERDATE"]
    formated_order_date = datetime.datetime.strptime(order_date, '%m/%d/%Y 0:00')
    month_val = row["MONTH_ID"]
    quarter_val = row["QTR_ID"]
    year_val = row["YEAR_ID"]

    mycursor = conn.cursor()
    q_select_year_id = """SELECT YearID FROM Years WHERE YearVal = %s"""
    param = (year_val,)
    mycursor.execute(q_select_year_id, param)
    result_select_year_id = mycursor.fetchall()
    if len(result_select_year_id) == 0:
        q_insert_year_id = "INSERT INTO Years (YearVal) VALUES (%s)"
        param = (year_val,)
        mycursor.execute(q_insert_year_id, param)
        conn.commit()
        year_id = mycursor.lastrowid
    elif len(result_select_year_id) == 1:
        year_id = result_select_year_id[0][0]
    else:
        raise Exception("Duplicate YearID")

    q_select_quarter_id = """SELECT QuarterID FROM Quarters WHERE QuarterVal = %s AND YearID = %s"""
    param = (quarter_val, year_id)
    mycursor.execute(q_select_quarter_id, param)
    result_select_quarter_id = mycursor.fetchall()
    if len(result_select_quarter_id) == 0:
        q_insert_quarter_id = "INSERT INTO Quarters (QuarterVal, YearID) VALUES (%s, %s)"
        param = (quarter_val, year_id)
        mycursor.execute(q_insert_quarter_id, param)
        conn.commit()
        quarter_id = mycursor.lastrowid
    elif len(result_select_quarter_id) == 1:
        quarter_id = result_select_quarter_id[0][0]
    else:
        raise Exception("Duplicate QuarterID")

    q_select_month_id = """SELECT MonthID FROM Months WHERE MonthVal = %s and QuarterID = %s"""
    param = (month_val, quarter_id)
    mycursor.execute(q_select_month_id, param)
    result_select_month_id = mycursor.fetchall()
    if len(result_select_month_id) == 0:
        q_insert_month = "INSERT INTO Months (MonthVal, QuarterID) VALUES (%s, %s)"
        param = (month_val, quarter_id)
        mycursor.execute(q_insert_month, param)
        conn.commit()
        month_id = mycursor.lastrowid
    elif len(result_select_month_id) == 1:
        month_id = result_select_month_id[0][0]
    else:
        raise Exception("Duplicate MonthID")

    q_select_orderDate_id = """SELECT OrderDateID FROM OrderDate WHERE Date = %s and MonthID = %s"""
    param = (formated_order_date, month_id)
    mycursor.execute(q_select_orderDate_id, param)
    result_select_orderDate_id = mycursor.fetchall()
    if len(result_select_orderDate_id) == 0:
        q_insert_orderDate = "INSERT INTO OrderDate (Date, MonthID) VALUES (%s, %s)"
        param = (formated_order_date, month_id)
        mycursor.execute(q_insert_orderDate, param)
        conn.commit()
        orderDate_id = mycursor.lastrowid
    elif len(result_select_orderDate_id) == 1:
        orderDate_id = result_select_orderDate_id[0][0]
    else:
        raise Exception("Duplicate OrderDateID")

    mycursor.close()
    return orderDate_id

def populate_extended_fact_table_orders(conn, row):
    order_number = row["ORDERNUMBER"]
    order_line_number = row["ORDERLINENUMBER"]
    quantity = row["QUANTITYORDERED"]
    price_each = row["PRICEEACH"]
    status = row["STATUS"]

    mycursor = conn.cursor()

    q_select_orders_id = """SELECT OrderNumber, OrderLineNumber FROM Orders 
                                WHERE OrderNumber = %s 
                                AND OrderLineNumber = %s """
    param = (order_number, order_line_number)
    mycursor.execute(q_select_orders_id, param)
    result_select_orders_id = mycursor.fetchall()
    if len(result_select_orders_id) == 0:
        q_insert_orders = """INSERT INTO Orders
                                (OrderNumber, OrderLineNumber, Quantity, PriceEach, Status)
                                VALUES (%s, %s, %s, %s, %s)"""
        param = (order_number, order_line_number, quantity, price_each, status)
        mycursor.execute(q_insert_orders, param)
        conn.commit()
        orders_ids = (order_number, order_line_number)
    elif len(result_select_orders_id) == 1:
        orders_ids = result_select_orders_id[0]
    else:
        raise Exception("Duplicate OrdersIDs")

    mycursor.close()
    return orders_ids

def populate_fact_table(conn, row, geoLocation_id, customer_id, order_ids, date_id, product_id):
    sales = row["SALES"]
    deal_size = row["DEALSIZE"]

    mycursor = conn.cursor()
    q_select_sales_id = """SELECT SalesID FROM Sales 
                            WHERE OrderNumber = %s AND OrderLineNumber = %s """
    param = (order_ids[0], order_ids[1])
    mycursor.execute(q_select_sales_id, param)
    result_select_sales_id = mycursor.fetchall()
    if len(result_select_sales_id) == 0:
        q_insert_sales = """INSERT INTO Sales
                            (OrderNumber, OrderLineNumber, GeoLocationID, CustomerID, DateID, Sales, DealSize, ProductID)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
        param = (order_ids[0], order_ids[1], geoLocation_id, customer_id, date_id, sales, deal_size, product_id)
        mycursor.execute(q_insert_sales, param)
        conn.commit()
        sales_id = mycursor.lastrowid
    elif len(result_select_sales_id) == 1:
        sales_id = result_select_sales_id[0][0]
    else:
        raise Exception("Duplicate SalesID")

    mycursor.close()
    return sales_id




def populate_db(source_csv_filename, db_name):
    with open(source_csv_filename, mode='r') as csv_read_file:
        csv_reader = csv.DictReader(csv_read_file)

        try:
            connection = mysql.connector.connect(host=host_name,
                                                 database=db_name,
                                                 user=my_username,
                                                 password=my_password,
                                                 auth_plugin='mysql_native_password')
            for row in csv_reader:
                for key, value in row.items():
                    row[key] = value.strip()
                    if row[key] == "":
                        row[key] == "N/A"
                city_id = populate_geo_dimension(connection, row)
                customer_id = populate_customer_dimension(connection, row)
                product_id = populate_product_dimension(connection, row)
                orderDate_id = populate_time_dimension(connection, row)
                orders_ids = populate_extended_fact_table_orders(connection, row)
                populate_fact_table(connection, row, geoLocation_id=city_id, customer_id=customer_id,
                                    order_ids=orders_ids, date_id=orderDate_id, product_id=product_id)
        except Error as e:
            print("Error while connecting to MySQL", e)
        finally:
            if (connection.is_connected()):
                connection.close()
                print("MySQL connection is closed")


if __name__ == "__main__":
    test_database_connection(host_name, db_name, my_username, my_password)

    source_csv_file = path.join(path_dir, source_csv)
    populate_db(source_csv_file, db_name)