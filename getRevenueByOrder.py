
# Import sys is for passing arguments
import sys

# function to read the file
def readFile (path):
    datafile = open(path).read().splitlines()
    return datafile

# function to get the revenue
def getRevenueforOrderID (oidata, oid):
    FilterItemsforOrder = filter(lambda x: int(x.split(",")[1]) == oid, oidata)
    GetOrderItemTotals = map(lambda y: float(y.split(",")[4]), FilterItemsforOrder)

    #import functools as ft
    GetRevenue = reduce(lambda a , y : a + y, GetOrderItemTotals)
    return GetRevenue

orderItemsPath = sys.argv[1]

orderItemsData = readFile(orderItemsPath)

GetRevenue = getRevenueforOrderID(orderItemsData , int(sys.argv[2]))

print GetRevenue
