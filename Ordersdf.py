
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.\
    builder.\
    appName('Orders DF').\
    getOrCreate()

print(spark)

orders = spark.read.csv("/Users/prajaya/data/retail_db/orders")

print(type(orders))

ordersDF = orders.toDF("order_id","order_date","order_customer_id","order_status").\
           select("order_id",substring("order_date",1,7).alias("order_yyyymm"),"order_customer_id","order_status")

print(type(ordersDF))

OrderStatusCounts = ordersDF.select("order_id","order_status").\
    groupBy("order_status").\
    agg(countDistinct("order_id").alias("Count of orders"))

OrderStatusCounts.show()