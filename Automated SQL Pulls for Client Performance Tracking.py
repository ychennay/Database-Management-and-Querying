#create the dataframe to store the output data in
orders_dataframe = pd.DataFrame(index=df[''], columns=salesforce.columns[8:])
proceeds_dataframe = pd.DataFrame(index=df[''], columns=salesforce.columns[8:])
pickups_dataframe = pd.DataFrame(index=df[''], columns=salesforce.columns[8:])
unique_customers_dataframe = pd.DataFrame(index=df[''], columns=salesforce.columns[8:])
cancelled_orders_dataframe = pd.DataFrame(index=df[''], columns=salesforce.columns[8:])
cancelled_proceeds_dataframe = pd.DataFrame(index=df[''], columns=salesforce.columns[8:])
cancelled_customers_dataframe = pd.DataFrame(index=df[''], columns=salesforce.columns[8:])

#create an error array to look up errors later
error_array = []
#two double loops: inner loop going down each row, then outer loop going three months out
for time_period in range(0,10):
    for row in range(len(df)):

        #any exception will be skipped, and the code will continue looping through
        try:

            restaurant_id = int(df.iloc[row, df.columns.get_loc("")])

            # if there is no data in the given cell (ie. the restaurant is not old enough to have three months of data,
            # then skip this and just write null in dataframe

            if str(df.iloc[row, 8 + time_period]) == 'nan' or str(df.iloc[row, 8 + time_period]) == '':
                orders_dataframe.iloc[row, time_period] = 'nan'
                proceeds_dataframe.iloc[row, time_period] = 'nan'

            else: #initiate the SQL pull

                start_date = df.iloc[row,8+time_period] #set the start and end date for the query
                end_date = df.iloc[row,7+time_period]

                #compose the primary SQL pull
                       ###REDACTED###
                       
                       
                class ChurnPull(object):
                    #establish connection to warehouse
                    connection = pymysql.connect(##REDACTED##)
                    
                    def createConnection(cls):
                        cursor = cls.connection.cursor()
                        return cursor

                    def pullQuery(cls, cursor, querytext):
                        cursor.execute(querytext)
                        resultQuery = cursor.fetchall()

                        for results in resultQuery:
                            restaurant_id = results[0]
                            restaurant_name = results[1]
                            order_total = results[2]

                            #convert the "none" variable type into 0s
                            if order_total == None:
                                order_total = 0


                            #save the output from SQL into local variable objects
                            order_volume = results[3]
                            pickup_volume = results[4]
                            unique_customers = results[5]

                        #compose the cancelled order query
                       ###REDACTED###
                       
                  
                        #execute a series of queries related to cancelled orders
                        cursor.execute(cancelled_query)
                        cancelQuery = cursor.fetchall()
                        for cancel_results in cancelQuery:
                            cancelled_order_total = cancel_results[0]

                            if cancelled_order_total== None:
                                cancelled_order_total= 0

                            cancelled_order_volume = cancel_results[1]
                            cancelled_unique_customers = cancel_results[2]
                        cursor.close()
                        cls.connection.close()
                        return order_volume, order_total, pickup_volume, unique_customers, cancelled_order_total, cancelled_order_volume, cancelled_unique_customers

                test = ChurnPull()
                query = test.createConnection()
                order_volume, order_total, pickup_volume, unique_customers, cancelled_order_total, cancelled_order_volume, \
                cancelled_unique_customers = test.pullQuery(query, querytext)

                orders_dataframe.iloc[row, time_period] = order_volume
                proceeds_dataframe.iloc[row, time_period]= order_total
                pickups_dataframe.iloc[row, time_period] = pickup_volume
                unique_customers_dataframe.iloc[row, time_period] = unique_customers
                cancelled_proceeds_dataframe.iloc[row, time_period] = cancelled_order_total
                cancelled_orders_dataframe.iloc[row, time_period] = cancelled_order_volume
                cancelled_customers_dataframe.iloc[row, time_period] = cancelled_unique_customers

                progress = (row + time_period * len(df)) / (float(len(df)) * 10) * 100
                progress = round(progress, 1)
                print("{0} % complete".format(progress))

        except Exception:
            print("****************************************************************************************************\n\n")
            print("Error Occurred at row {0} and time period {1}".format(row, time_period))
            error_array.append(row)
            time.sleep(10)
            pass
