# Databricks notebook source
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark = SparkSession.\
       builder.\
       getOrCreate()
conf = SparkConf()
#sc = SparkContext(conf=conf)

# COMMAND ----------

# For unstructured data Using RDDs
ordersRDD = sc.textFile("/FileStore/tables/part_00000-6a99e")
ordersMapRDD = ordersRDD.map(lambda rec: (rec.split(",")[3],1))
ordersStatusRDDCount = ordersMapRDD.reduceByKey(lambda x,y : x + y)
ordersStatusRDDSorted = ordersStatusRDDCount.sortBy(lambda z: z[1], ascending=False)
ordersStatusRDDDataFrame = spark.createDataFrame(ordersStatusRDDSorted)
ordersStatusRDDDataFrame.toDF("order_status", "count").show()

# COMMAND ----------

# for known CSV files without headers Using Spark Dataframes
ordersCSV = spark.read.csv("/FileStore/tables/part_00000-6a99e").toDF("order_id","order_date","order_customer_id","order_status")
ordersCSVSchema = ordersCSV.\
              withColumn("order_id",ordersCSV.order_id.cast(IntegerType())).\
              withColumn("order_customer_id",ordersCSV.order_customer_id.cast(IntegerType()))
ordersCSVData = ordersCSVSchema.select("order_status","order_id").groupBy("order_status").agg(count("order_id").alias("count"))
ordersCSVDataSorted = ordersCSVData.sort(col("count").desc())
ordersCSVDataSorted.show()

# COMMAND ----------

# for all kinds of files => Declare Schema, attach schema to structure and load table/ Create data frame and temp table
ordersSchema =  StructType([\
                StructField("order_id", IntegerType(), True), \
                StructField("order_date", StringType(), True), \
                StructField("order_customer_id", IntegerType(), True), \
                StructField("order_status", StringType(), True),\
                ])

orders = spark.read.format("csv")\
  .option("header", "true")\
  .schema(ordersSchema)\
  .load("/FileStore/tables/part_00000-6a99e")

orders.createOrReplaceTempView("orders")

results = spark.sql("select order_status, count(*) from orders group by order_status order by 2 desc")

results.show()

# COMMAND ----------

