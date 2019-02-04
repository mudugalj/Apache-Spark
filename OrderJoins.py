# Date Created : 16 October 2018

from pyspark import SparkConf, SparkContext
from operator import add
#import pandas as pd

print "Setting spark context......."
sc = SparkContext(master="local", appName="Spark Demo")

print "Building orders RDD......."
orders = sc.textFile("C:\\data\\retail_db\\orders")
ordersFinal = orders.filter(lambda x : x.split(",")[3] in ['COMPLETE', 'CLOSED'])
ordersJoinData = ordersFinal.map(lambda p: (int(p.split(",")[0]), p.split(",")[3]))

print "Building order items RDD......."
# Getting all order items tha are greater than 100 dollars
orderItems = sc.textFile("C:\\data\\retail_db\\order_items")
orderItemsFinal = orderItems.filter(lambda y: float(y.split(",")[4]) >= 100.00)
orderItemsJoinData = orderItemsFinal.map(lambda p: (int(p.split(",")[0]), float(p.split(",")[4])))

# Joining
print "Joining data sets orders and order items......."
ordersJoined=ordersJoinData.join(orderItemsJoinData)

#Left Outer Join
#ordersLeftJoined=ordersJoinData.leftOuterJoin(orderItemsJoinData)

# Map required values from key
print "Bringing status map......."
OrdersStatusMap=ordersJoined.map(lambda (k,(v1,v2)): (v1,v2))

# Status SUMs & COUNTS
order_status_sum=sorted(OrdersStatusMap.reduceByKey(add).collect())
order_status_counts=sorted(OrdersStatusMap.countByKey().items())

print "Bring result......"
print order_status_sum
print order_status_counts

# Convert into dataframe
#OrderCountsDF = pd.DataFrame(order_status_counts, columns = ['order_status','total_count'])
#OrderSUMDF = pd.DataFrame(order_status_sum, columns = ['order_status','total_sum'])

# Mergining or joining data frame
#OrderStatusFinalResult = pd.merge(OrderCountsDF, OrderSUMDF, how='inner')

#print OrderStatusFinalResult
