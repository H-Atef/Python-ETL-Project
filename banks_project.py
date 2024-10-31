import numpy as np
import pandas as pd
import datetime as dt
import sqlite3 as sq
from bs4 import BeautifulSoup
import requests as rq



class ETL_PIPELINE:

    def __init__(self,url,db_name):
        self.url=url
        self.sql_con=sq.connect(db_name)
        self.db_name=db_name
        
        

    def log_progress(self,message):
        # This function logs the mentioned message of a given stage of the
        # code execution to a log file. Function returns nothing
         timestamp_format="%Y-%h-%d-%H:%M:%S"
         dt_now=dt.datetime.now()
         time_stamp=dt.datetime.strftime(dt_now,timestamp_format)
         with open("code_log.txt","a") as f:
             f.write(time_stamp+" : "+message+"\n")



    def extract(self,t_num=0,table_attribs=None):
        # This function aims to extract the required
        # information from the website and save it to a data frame. 
        # The function returns the data frame for further processing.

        if table_attribs is None:
            table_attribs=["",""]

        req=rq.get(self.url).text
        soup=BeautifulSoup(req,"html5lib")

        #Getting first table only 
        #(t_num or table number identified as 0 if not entered)
        table=soup.find_all("table")[t_num]

        bank_names=[]
        market_caps=[]

        table_rows=table.find_all("tr")
        for row in table_rows[1:]:
            
            record_cells=row.text.split("\n")
            #for removing empty strings
            record_cells=[ x for x in record_cells if x]

            bank_name=record_cells[1]
            market_cap=float(record_cells[2])

            bank_names.append(bank_name)
            market_caps.append(market_cap)   

        df=pd.DataFrame({table_attribs[0]:bank_names,
                         table_attribs[1]:market_caps})

        return df

    def transform(self,df=None, csv_path=None):
        # This function accesses the CSV file for exchange rate
        # information, and adds three columns to the data frame, each
        # containing the transformed version of Market Cap column to
        # respective currencies


        if df is None or csv_path is None:
            pass

        else:
            ex_rate_df=pd.read_csv(csv_path)
            ex_rate_dict=ex_rate_df.set_index('Currency').to_dict()['Rate']

            df['MC_GBP_Billion'] = [np.round(x*float(ex_rate_dict['GBP']),2) for x in df['MC_USD_Billion']]
            df['MC_INR_Billion'] = [np.round(x*float(ex_rate_dict['INR']),2) for x in df['MC_USD_Billion']]
            df['MC_EUR_Billion'] = [np.round(x*float(ex_rate_dict['EUR']),2) for x in df['MC_USD_Billion']]

            return df

    def load_to_csv(self,df=None, output_path=None):
        # This function saves the final data frame as a CSV file in
        # the provided path. Function returns nothing.
        if df is None:
            pass
        else:
            df.to_csv(output_path,index=False)


    def load_to_db(self,df=None, table_name=None):
        # This function saves the final data frame to a database
        # table with the provided name. Function returns nothing.
        if df is None or table_name is None:
            pass

        else:
            self.sql_con=sq.connect(self.db_name)
            df.to_sql(table_name,self.sql_con,index=False,if_exists="replace")
            self.sql_con.close()


    def run_query(self,query_statement=None):
        # This function runs the query on the database table and
        # prints the output on the terminal. Function returns nothing.
         
         if query_statement is None:
             pass 
         
         else:
            self.sql_con=sq.connect(self.db_name)
            cur = self.sql_con.cursor()
            cur.execute(query_statement)
            rows = cur.fetchall()
            col_names=[x[0] for x in cur.description]

            #Inserting and printing the output using DataFrame
            df=pd.DataFrame(rows)
            df.columns=col_names
            print(df.to_string(index=False))
            print("\n")
            cur.close()
            self.sql_con.close()



db_name="Bank.db"

url="https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"

etl_obj=ETL_PIPELINE(url,db_name)


etl_obj.log_progress("Preliminaries complete. Initiating ETL process")
df=etl_obj.extract(table_attribs=["Name","MC_USD_Billion"])
etl_obj.log_progress("Data extraction complete. Initiating Transformation process")


df_t=etl_obj.transform(df,"exchange_rate.csv")
etl_obj.log_progress("Data transformation complete. Initiating Loading process")

etl_obj.load_to_csv(df_t,"MC_Largest_Banks.csv")
etl_obj.log_progress("Data saved to CSV file")

etl_obj.log_progress("SQL Connection initiated")

etl_obj.load_to_db(df_t,"largest_banks")
etl_obj.log_progress("Data loaded to Database as a table, Executing queries")

etl_obj.run_query("SELECT * FROM Largest_banks")
etl_obj.run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks")
etl_obj.run_query("SELECT Name from Largest_banks LIMIT 5")
etl_obj.log_progress("Process Complete")

etl_obj.log_progress("Server Connection closed")


