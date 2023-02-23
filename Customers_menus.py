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
