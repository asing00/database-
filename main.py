#import the libraries needed

import os
import numpy as np
import pandas as pd
import psycopg2

df = pd.read_csv("Customer Contracts$.csv")
df = df.head()


# convert table name to lower case, remove spaces and special characters eg need _

filename = "Customer Contracts$"

cleaned_table_name = filename.lower().replace(" ","_").replace("-","_")  \
                    .replace(r"/","_").replace("\\","_").replace("$","").replace("%","") \
                    .replace("?","")


# clean header names like above and convert data type so can be used with SQL

df.columns = [z.lower().replace(" ","_").replace("-","_")  \
                    .replace(r"/","_").replace("\\","_").replace("$","").replace("%","") \
                    .replace("?","") for z in df.columns]


replacement = {
        'timedelta64[ns]': 'varchar',
        'object': 'varchar',
        'float64': 'float',
        'int64': 'int',
        'datetime64': 'timestamp'}

column_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(df.columns, df.dtypes.replace(replacement)))

print(column_str)

#enter your database details
def upload_to_db(host, dbname, user, password, tbl_name, col_str, file, dataframe, dataframe_columns):

    connection_string = "host=localhost dbname=some-postgres user= password=mysecretpassword" #host, dbname, user, password 



    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()
    print('opened database successfully')


#drop table with same name
    cursor.execute("drop table if exists customer_contracts;") 

#create table
    cursor.execute("create table customer_contracts \
    (customer_name varchar, start_date varchar, end_date varchar, contract_amount_m float, invoice_sent varchar, paid varchar")

#insert values into the table
# save dataframe to csv
    df.to_csv("customer_contracts.csv", header= df.columns, index= False, encoding="utf-8")

# open csv file, save it as object
    myfile = open("customer_contracts.csv")
    print("file opened")

# and upload to the database

    sql_statement = """
    COPY customer_contracts FROM STDIN WITH
        CSV
        HEADER
        DELIMITER AS ','
    """

    cursor.copy_expert(sql=sql_statement, file = myfile)
    print("file copied to database")

    cursor.execute("grant select on table customer_contracts to public")
    conn.commit()

    cursor.close()
    print("table customer_contrats import to database completed")

    return
