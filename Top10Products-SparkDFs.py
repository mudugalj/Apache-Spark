from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from pyspark.sql import Row
from time import time

sc = SparkContext(master="local",appName="Spark Demo")
sc.setLogLevel("ERROR")

spark = SparkSession     \
    .builder     \
    .appName("Python Spark SQL basic example")     \
    .config("spark.some.config.option", "some-value")     \
    .getOrCreate()

t0 = time()


ordersFile = sc.textFile("C:\\data\\retail_db\\orders")
ordersData = ordersFile.map(lambda rec: Row(order_id=rec.split(",")[0], order_status=rec.split(",")[3]))
orders = spark.createDataFrame(ordersData)

orderItemsFile = sc.textFile("C:\\data\\retail_db\\order_items")
orderItemsData = orderItemsFile.map(lambda rec: Row(order_id=rec.split(",")[1], product_id=rec.split(",")[2], sub_total=rec.split(",")[4]))
orderitems = spark.createDataFrame(orderItemsData)

productsFile = sc.textFile("C:\\data\\retail_db\\products")
productsData = productsFile.map(lambda rec: Row(product_id=rec.split(",")[0],product_name=rec.split(",")[2]))
products = spark.createDataFrame(productsData)

import pyspark.sql.functions as func

#orders.groupBy("order_status").agg(func.count("order_id")).show()

orders1 = orders.where("NOT(order_status == 'CANCELED')")
orders2 = orders1.where("NOT(order_status == 'SUSPECTED_FRAUD')")

#orders2.groupBy("order_status").agg(func.count("order_id")).show()

OOI = orders2.join(orderitems, orders2.order_id == orderitems.order_id, 'inner').select(orderitems.product_id, orderitems.sub_total)
OrderTotals = OOI.groupBy("product_id").agg(func.sum("sub_total").alias("revenue"))

Result = products.join(OrderTotals, products.product_id == OrderTotals.product_id, 'inner').select(products.product_id, products.product_name, OrderTotals.revenue)

from pyspark.sql.functions import desc

results = Result.sort(desc('revenue')).limit(10)

results.write.format("csv").save("C:\\data\\results_1")

tt = str(time() - t0)

print " Operation performed in " + tt + " seconds"

