import findspark
findspark.init()
import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType
import pandas as pd
import datetime


# Establish a connection to the database
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="password",
  database="creditcard_capstone"
)
# Create a cursor object to execute SQL queries
mycursor = mydb.cursor()

def transactions_zip_month_year(zip_code, month, year):
    # format the month and year parameters as a string in the format "YYYYMM"
    timeid = year + month
    
    sql = "SELECT CDW_SAPP_CREDIT.CREDIT_CARD_NO, CDW_SAPP_CREDIT.TIMEID, CDW_SAPP_CREDIT.TRANSACTION_TYPE, CDW_SAPP_CREDIT.TRANSACTION_VALUE \
    FROM CDW_SAPP_CREDIT \
    JOIN CDW_SAPP_CUSTOMER ON CDW_SAPP_CREDIT.CUST_SSN = CDW_SAPP_CUSTOMER.SSN \
    WHERE CDW_SAPP_CUSTOMER.CUST_ZIP = %s AND CDW_SAPP_CREDIT.TIMEID LIKE %s \
    ORDER BY CDW_SAPP_CREDIT.TIMEID DESC"
    
    val = (zip_code, f"{timeid}%")
    mycursor.execute(sql, val)
    result = mycursor.fetchall()
    for row in result:
        print(row)

def transactions_by_type(transaction_type):
    sql = "SELECT COUNT(*), SUM(TRANSACTION_VALUE) FROM CDW_SAPP_CREDIT \
           WHERE TRANSACTION_TYPE = %s"
    val = (transaction_type,)
    mycursor.execute(sql, val)
    result = mycursor.fetchone()
    print("Number of transactions for type", transaction_type, "is", result[0])
    print("Total value of transactions for type", transaction_type, "is", result[1])

def transactions_by_state(state):
    sql = "SELECT COUNT(*), SUM(TRANSACTION_VALUE) FROM CDW_SAPP_CREDIT \
           JOIN CDW_SAPP_BRANCH ON CDW_SAPP_CREDIT.BRANCH_CODE = CDW_SAPP_BRANCH.BRANCH_CODE \
           WHERE CDW_SAPP_BRANCH.BRANCH_STATE = %s"
    val = (state,)
    mycursor.execute(sql, val)
    result = mycursor.fetchone()
    print("Number of transactions for branches in state", state, "is", result[0])
    print("Total value of transactions for branches in state", state, "is", result[1])



def display_menu():
    print("Transaction Menu")
    print("1. Display transactions by zip, month, and year")
    print("2. Display transactions by type")
    print("3. Display transactions by state")
    print("4. Exit")

while True:
    display_menu()
    selection = input("Please enter your selection based on the numeric values above: ")
    
    try:
        selection = int(selection)
    except:
        print("Please choose one of the numeric values above")
        continue

    if selection < 1:
        print("Please enter a number that is greater than 0")
        continue

    if selection == 1:
        zipcode = input("Enter zip code: ")
        month = input("Enter month (01-12): ")
        year = input("Enter year (YYYY): ")
        transactions_zip_month_year(zipcode, month, year)
    elif selection == 2:
        transaction_type = input("Enter transaction type: ")
        transactions_by_type(transaction_type)
    elif selection == 3:
        state = input("Enter state: ")
        transactions_by_state(state)
    elif selection == 4:
        break
    else:
        print("Please make a valid selection")