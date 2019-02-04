from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Initiate Spark Session
spark = SparkSession.builder.\
    appName('RDD demo').\
    getOrCreate()

sc = spark.sparkContext

# Read data
orders = sc.textFile("/Users/prajaya/data/retail_db/orders")
ordersMap = orders.map(lambda l : Row(l.split(",")[2],l.split(",")[3]))

# Sneak peak the data
#for i in ordersMap.take(3):
#    print(i)

# Defining the Schema
schemaString = "order_id,order_status"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(",")]
schema = StructType(fields)

# Apply the schema to the RDD.
schemaOrders = spark.createDataFrame(ordersMap, schema)

# Creates a temporary view using the DataFrame
schemaOrders.createOrReplaceTempView("orders")

# SQL can be run over DataFrames that have been registered as a table.
results = spark.sql("SELECT order_status, count(*) as order_count \
                     FROM orders group by order_status")
results.show()