
# Import sys is for passing arguments
import sys

# function to read the file
def readFile (path):
    datafile = open(path).read().splitlines()
    return datafile

orderItemsPath = sys.argv[1]
orderItemsData = readFile(orderItemsPath)

GetOrdersItems = map(lambda x: (int(x.split(",")[0]), int(x.split(",")[1]), float(x.split(",")[4]), orderItemsData)

import pandas as pd
from pandas import dataframe

df = pd.DataFrame(GetOrdersItems, columns=['order_id','order_item_id','revenue'])

print df[df['order_id'] == sys.argv[2]].groupby('order_item_id')['revenue'].sum()
print df[df['order_id'] == sys.argv[2]].groupby('order_id') ['order_item_id'].count()