import findspark
findspark.init()
import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType
import pandas as pd
import datetime
spark = SparkSession.builder.master("local[*]").appName("Menus").getOrCreate()

# Establish a connection to the database
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="password",
  database="creditcard_capstone"
)
# Create a cursor object to execute SQL queries
mycursor = mydb.cursor()


def customer_account_info(credit_card_no):
    sql = "SELECT CDW_SAPP_BRANCH.BRANCH_NAME, CDW_SAPP_CUSTOMER.FIRST_NAME, CDW_SAPP_CUSTOMER.LAST_NAME, CDW_SAPP_CUSTOMER.ADDRESS, CDW_SAPP_CUSTOMER.CUST_CITY, CDW_SAPP_CUSTOMER.CUST_STATE, CDW_SAPP_CUSTOMER.CUST_ZIP, CDW_SAPP_CREDIT.CREDIT_CARD_NO, CDW_SAPP_CREDIT.CUST_SSN \
    FROM CDW_SAPP_CREDIT \
    JOIN CDW_SAPP_CUSTOMER ON CDW_SAPP_CREDIT.CUST_SSN = CDW_SAPP_CUSTOMER.SSN \
    JOIN CDW_SAPP_BRANCH ON CDW_SAPP_CREDIT.BRANCH_CODE = CDW_SAPP_BRANCH.BRANCH_CODE \
    WHERE CDW_SAPP_CREDIT.CREDIT_CARD_NO = %s \
    GROUP BY CDW_SAPP_CREDIT.CREDIT_CARD_NO"
    val = (credit_card_no,)
    mycursor.execute(sql, val)
    result = mycursor.fetchall()
    for row in result:
        print(row)

def modify_customer_account_details(SSN, new_address, new_city, new_state, new_zip):
    sql = "UPDATE CDW_SAPP_CUSTOMER \
           SET ADDRESS = %s, CUST_CITY = %s, CUST_STATE = %s, CUST_ZIP = %s \
           WHERE SSN = %s"
    val = (new_address, new_city, new_state, new_zip, SSN)
    mycursor.execute(sql, val)
    mydb.commit()
    print(mycursor.rowcount, "record(s) affected")

def generate_monthly_bill(credit_card_no, month, year):
    # format the month and year parameters as a string in the format "YYYYMM"
    timeid = year + month
    
    # query to calculate the monthly bill
    sql = "SELECT TRANSACTION_TYPE, SUM(TRANSACTION_VALUE) AS TOTAL_SPEND FROM CDW_SAPP_CREDIT \
           WHERE CREDIT_CARD_NO = %s AND TIMEID LIKE %s \
           GROUP BY TRANSACTION_TYPE"
    
    # execute the query
    val = (credit_card_no, f"{timeid}%")
    mycursor.execute(sql, val)
    result = mycursor.fetchall()
    
    # calculate the total bill amount
    total_bill = 0
    for row in result:
        if row[0] == "C":
            total_bill -= row[1]
        else:
            total_bill += row[1]
    
    # print the bill information
    print("Credit card number:", credit_card_no)
    print("Billing period:", month, "/", year)
    print("Total amount due:", total_bill)

def transactions_by_date_range(SSN, start_timeid, end_timeid):
    mycursor = mydb.cursor()
    sql = """
        SELECT SUBSTR(CDW_SAPP_CREDIT.TIMEID, 1, 4) AS YEAR, SUBSTR(CDW_SAPP_CREDIT.TIMEID, 5, 2) AS MONTH, 
               SUBSTR(CDW_SAPP_CREDIT.TIMEID, 7, 2) AS DAY, CDW_SAPP_CREDIT.TRANSACTION_TYPE
        FROM CDW_SAPP_CREDIT
        JOIN CDW_SAPP_CUSTOMER ON CDW_SAPP_CREDIT.CUST_SSN = CDW_SAPP_CUSTOMER.SSN
        WHERE CDW_SAPP_CREDIT.CUST_SSN = %s 
          AND CDW_SAPP_CREDIT.TIMEID >= %s 
          AND CDW_SAPP_CREDIT.TIMEID <= %s 
        ORDER BY YEAR DESC, MONTH DESC, DAY DESC
    """
    val = (SSN, start_timeid, end_timeid)
    mycursor.execute(sql, val)
    result = mycursor.fetchall()
    for row in result:
        print(row)
