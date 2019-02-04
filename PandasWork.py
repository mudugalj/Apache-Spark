import sys
import pandas as pd

df_orders = pd.read_csv(sys.argv[1], \
                 header=None, \
                 names=['order_id', 'order_date','customer_id','order_status'],\
                 usecols=['order_id','order_status'])

#df_orders = df_orders[df_orders['order_status'].isin(['COMPLETE','CLOSED'])]

#df_orders = df_orders[df_orders['order_status'].isin(['COMPLETE','CLOSED'])]\
#    .groupby('order_status').count()

#print df_orders

df_orders_items = pd.read_csv(sys.argv[2], \
                        header=None, \
                        names=['order_item_id','order_id','product_id','qty','total_value','unit_value'], \
                        usecols=['order_id','total_value'])

#df_orders_items = df_orders_items[df_orders_items['order_id'] == 2].groupby('order_id').sum()

#print df_orders_items

result_all = pd.merge(df_orders,df_orders_items, how='left', on='order_id')

#print result_all

results_Invalid = result_all[result_all['total_value'].isnull()]

print results_Invalid.groupby('order_status').count()

print results_Invalid[results_Invalid['order_status'].isin(['COMPLETE','CLOSED'])]\
    .groupby('order_status')\
    .count()


