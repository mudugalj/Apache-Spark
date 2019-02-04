
#Import SparkSession library
from pyspark.sql import SparkSession

# Initiate Spark Session
spark = SparkSession.builder.\
    appName('JSON demo').\
    getOrCreate()

# Read data
ordersDF = spark.read.json("/Users/prajaya/data/retail_db_json/orders")
print("Number of records before filter", ordersDF.count())

#Filter data
ordersDF = ordersDF.filter(ordersDF['order_status'].isin('CLOSED',"COMPLETE"))
print("Number of records after filter", ordersDF.count())

#Group data
ordersGrouped = ordersDF.groupby('order_status').count()

#Show data and Type
ordersGrouped.show()
print("The type of ordersGrouped is ", type(ordersGrouped))

#Create Temp table in Spark Catalogue / List tables / Show Type
ordersGrouped.createOrReplaceTempView("validOrders")
print(spark.catalog.listTables())
sqlDF = spark.sql("SELECT * FROM validOrders")
sqlDF.show()
print("The type of sqlDF is", type(sqlDF))

