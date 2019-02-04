
# Import Python Libraries


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Initiate Spark Session

spark = SparkSession.\
    builder.\
    appName('Orders-Orders Items DF').\
    getOrCreate()

#print(spark)

# Load orders data
orders = spark.read.csv("/Users/prajaya/data/retail_db/orders")

#print(type(orders))

# Format data
ordersDF = orders.toDF("order_id","order_date","order_customer_id","order_status").\
           select("order_id",substring("order_date",1,4).alias("order_yyyy"),"order_customer_id","order_status").\
           filter((col("order_status").isin("CLOSED",'COMPLETE')))

ordersDF = ordersDF.\
            withColumn("order_id",ordersDF.order_id.cast(IntegerType())).\
            withColumn("order_customer_id", ordersDF.order_customer_id.cast(IntegerType())).\
            withColumn("order_yyyy", ordersDF.order_yyyy.cast(StringType()))


# Load order items data
orderItems = spark.read.csv("/Users/prajaya/data/retail_db/order_items")

orderItemsDF = orderItems.toDF("order_item_id","order_item_order_id","order_item_product_id",
                               "order_item_quantity","order_item_subtotal","order_item_product_price")


#print(type(ordersDF))

OrderStatusCounts = ordersDF.select("order_id","order_yyyy","order_status").\
    groupBy("order_yyyy","order_status").\
    agg(countDistinct("order_id").alias("Count of orders"))

#OrderStatusCounts.show()

# Joining orders and order Items , calculate revenue by order status by each year

RevenueByOrderStatus = ordersDF.join(orderItemsDF, ordersDF.order_id == orderItemsDF.order_item_order_id, how='inner') \
    .select(ordersDF.order_yyyy, ordersDF.order_status, orderItemsDF.order_item_order_id,orderItemsDF.order_item_subtotal) \
    .groupBy("order_yyyy","order_status")\
    .agg(countDistinct("order_item_order_id").alias("Count of Distinct Orders"),\
         sum("order_item_subtotal").alias("Revenue"))

#RevenueByOrderStatus.show()

#print(RevenueByOrderStatus.count())

RevenueByOrderStatus.write \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost/company")\
    .option("dbtable", "company.RevenueByOrderStatus") \
    .option("user", "root") \
    .option("password", "******") \
    .option("SaveMode","overwrite")\
    .saveAsTable("RevenueByOrderStatus")