import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import configparser

# import utilities
from common.readData import readData
read = readData()

#read config
config =configparser.ConfigParser()
config.read(r'..\input_files\config.ini')
inputLocation = config.get('path','input_path')

df_employee20220521 = inputLocation + 'employee_2022_05_21.csv'

#Create spark session
spark = SparkSession.builder.appName('scdproject').getOrCreate()

df_employee = read.readcsv(spark=spark,path=df_employee20220521,header=True,multiLine=True)

df_employee.show(truncate=15)
df_employee.printSchema()

