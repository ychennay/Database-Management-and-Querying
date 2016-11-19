#I redacted pieces of this code that contained sensitive information, but this was used to run an inner loop through Python onto a MySQL database warehouse
# and ultimately report on the lagging three-month performance of restaurants. This data was then ultimately used to train different
# predictive analytics models to forecast future customer churn

import os
import math
import numpy as np
import pandas as pd
import pymysql
from datetime import datetime


os.chdir("/Users/yuchen/Desktop/Working Data/Churn Forecasting")

#import CSV file
df = pd.read_csv("Churn Forecasting.csv")

orders_dataframe = pd.DataFrame(index=df['REDACTED'], columns=['Last Month', 'Second to Last Month', 'Third to Last Month'])
proceeds_dataframe = pd.DataFrame(index=df['REDACTED'], columns=['Last Month', 'Second to Last Month', 'Third to Last Month'])

for time_period in range(0,3):
    for row in range(10):
        restaurant_id = int(df.loc[row,'REDACTED'])

        #checks if restaurants were in existence in the specific datetime period
        if str(df.iloc[row, 6 + time_period]) == 'nan':
            orders_dataframe.iloc[row, time_period] = 'nan'
            proceeds_dataframe.iloc[row, time_period] = 'nan'

        else:

            start_date = df.iloc[row,6+time_period]
            end_date = df.iloc[row,5+time_period]

            querytext = # this is where you would put in your query, redacted to hide sensitive information

            class ChurnPull(object):
                connection = pymysql.connect(host='127.0.0.1', port=XXX, user='XXXX', passwd='XXXX', db='XXX')

                def createConnection(cls):
                    cursor = cls.connection.cursor()
                    return cursor

                def pullQuery(cls, cursor, querytext):
                    cursor.execute(querytext)
                    print("SQL query executed")
                    resultQuery = cursor.fetchall()

                    for results in resultQuery:
                        restaurant_name = results[1]
                        order_volume = results[2]
                        order_total = results[3]
                  
                    cursor.close()
                    cls.connection.close()
                    return order_volume, order_total
