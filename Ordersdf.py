
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


ordersDF = ordersDF.\
            withColumn("order_id",ordersDF.order_id.cast(IntegerType())).\
            withColumn("order_customer_id", ordersDF.order_customer_id.cast(IntegerType()))


orderItems = spark.read.csv("/Users/prajaya/data/retail_db/order_items")

orderItemsDF = orderItems.toDF("order_item_id","order_item_order_id","order_item_product_id",
                               "order_item_quantity","order_item_subtotal","order_item_product_price")


print(type(ordersDF))

OrderStatusCounts = ordersDF.select("order_id","order_status").\
    groupBy("order_status").\
    agg(countDistinct("order_id").alias("Count of orders"))

OrderStatusCounts.show()

RevenueByOrderStatus = ordersDF.join(orderItemsDF, ordersDF.order_id == orderItemsDF.order_item_order_id, how='left') \
    .select(ordersDF.order_status, orderItemsDF.order_item_order_id,orderItemsDF.order_item_subtotal) \
    .groupBy("order_status")\
    .agg(countDistinct("order_item_order_id").alias("Count of Distinct Orders"),\
         sum("order_item_subtotal").alias("Revenue"))

RevenueByOrderStatus.show()
