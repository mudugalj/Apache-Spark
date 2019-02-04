from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from time import time

sc = SparkContext(master="local",appName="Spark Demo")
sc.setLogLevel("ERROR")

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

t0 = time()

orders = sc.textFile("C:\\data\\retail_db\\orders")
order_items = sc.textFile("C:\\data\\retail_db\\order_items")
products = sc.textFile("C:\\data\\retail_db\\products")

orders_map = orders\
    .map(lambda x : (int(x.split(",")[0]),x.split(",")[3]))
order_items_map = order_items\
    .map(lambda x : (int(x.split(",")[1]), int(x.split(",")[2]),float(x.split(",")[4])))
products_map = products\
    .map(lambda x : (int(x.split(",")[0]), x.split(",")[2]))

OrdersschemaString = "order_id order_status"
Ordersfields = [StructField(field_name, StringType(), True) for field_name in OrdersschemaString.split()]
ordersschema = StructType(Ordersfields)
OrderItemsschemaString = "order_item_order_id order_item_product_id order_item_subtotal"
OrderItemsfields = [StructField(field_name, StringType(), True) for field_name in OrderItemsschemaString.split()]
orderItemsschema = StructType(OrderItemsfields)
ProductsschemaString = "product_id product_name"

Productsfields = [StructField(field_name, StringType(), True) for field_name in ProductsschemaString.split()]
Productsschema = StructType(Productsfields)
schemaOrders = spark.createDataFrame(orders_map, ordersschema)
schemaOrderItems = spark.createDataFrame(order_items_map, orderItemsschema)
schemaProduct = spark.createDataFrame(products_map, Productsschema)

schemaOrders.createOrReplaceTempView("orders")
schemaOrderItems.createOrReplaceTempView("order_items")
schemaProduct.createOrReplaceTempView("products")

results = spark.sql("select p.product_id, p.product_name, r.revenue \
                    from products p inner join \
                    (select oi.order_item_product_id, sum(cast(oi.order_item_subtotal as float)) as revenue \
                    from order_items oi inner join orders o \
                    on oi.order_item_order_id = o.order_id \
                    where o.order_status <> 'CANCELED' \
                    and o.order_status <> 'SUSPECTED_FRAUD' \
                    group by order_item_product_id) r \
                    on p.product_id = r.order_item_product_id \
                    order by r.revenue desc \
                    limit 10")
#results.show()
results.write.format("csv").save("C:\\data\\results_1")

tt = str(time() - t0)

print " Operation performed in " + tt + " seconds"
