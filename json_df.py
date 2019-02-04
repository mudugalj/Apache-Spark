
#Import SparkSession library
from pyspark.sql import SparkSession

# Initiate Spark Session
spark = SparkSession.builder.\
    appName('JSON demo').\
    getOrCreate()

# Read data
ordersDF = spark.read.json("/Users/prajaya/data/retail_db_json/orders")
print("Number of records before filter", ordersDF.count())

#Print Schema
ordersDF.printSchema()

#Filter data
ordersDF = ordersDF.filter(ordersDF['order_status'].isin('CLOSED',"COMPLETE"))
print("Number of records after filter", ordersDF.count())

#Group data
ordersGrouped = ordersDF.groupby('order_status').count()

#Show data and Type
ordersGrouped.show()
print("The type of ordersGrouped is ", type(ordersGrouped))

#Create Local Temp table in Spark Catalogue / List tables / Show Type
ordersGrouped.createOrReplaceTempView("localValidOrders")
print(spark.catalog.listTables())
localSqlDF = spark.sql("SELECT * FROM localValidOrders")
localSqlDF.show()
print("The type of localSqlDF is", type(localSqlDF))

#Create GLobal Temp table in Spark Catalogue / List tables / Show Type
ordersGrouped.createGlobalTempView("globalValidOrders")
globalSqlDF = spark.sql("SELECT * FROM global_temp.globalValidOrders")
globalSqlDF.show()
print("The type of globalSqlDF is", type(globalSqlDF))

