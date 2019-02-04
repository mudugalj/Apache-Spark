
from pyspark import SparkConf, SparkContext
from pyspark import Row
from pyspark.sql import SparkSession
from time import time

sc = SparkContext(master='local', appName="RDD functions")
sc.setLogLevel("ERROR")

spark = SparkSession     \
    .builder     \
    .appName("Python Spark SQL basic example")     \
    .config("spark.some.config.option", "some-value")     \
    .getOrCreate()

spark.conf.set("spark.executor.memory", '500MB')
spark.conf.set('spark.executor.cores', '2')


t0 = time()

orders = sc.textFile("C:\\data\\retail_db\\orders")
orderitems = sc.textFile("C:\\data\\retail_db\\order_items")
products = sc.textFile("C:\\data\\retail_db\\products")

ordersFiltered = orders.filter(lambda f : f.split(",")[3] not in ['CANCELED','SUSPECTED_FRAUD'])

ordersMap = ordersFiltered.map(lambda y : Row(order_id=y.split(",")[0],                                      order_status=y.split(",")[3]))
orderItemsMap = orderitems.map(lambda z : Row(order_id=z.split(",")[1],                                               product_id=z.split(",")[2],                                              order_revenue=z.split(",")[4]))
productsMap = products.map(lambda x : Row(product_id = x.split(",")[0],                                           product_desc = x.split(",")[2]))

ordersDF = spark.createDataFrame(ordersMap)
orderitemsDF = spark.createDataFrame(orderItemsMap)
productsDF = spark.createDataFrame(productsMap)

ordersDF.createOrReplaceTempView("orders")
orderitemsDF.createOrReplaceTempView("order_items")
productsDF.createOrReplaceTempView("products")

results = spark.sql("select p.product_id, p.product_desc, r.revenue\
                    from products p inner join\
                    (select oi.product_id, sum(cast(oi.order_revenue as float)) as revenue\
                    from order_items oi inner join orders o\
                    on oi.order_id = o.order_id\
                    group by product_id) r\
                    on p.product_id = r.product_id\
                    order by r.revenue desc\
                    limit 10")

results.write.format("csv").save("C:\\data\\results2")

tt = str(time() - t0)

print " Operation performed in " + tt + " seconds"
