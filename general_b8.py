# -*- coding: utf-8 -*-
"""
Created on : 20 MAr, 2021
Primary author: sifat
"""

import sys
import json
#import numpy as np
import pandas.io.sql as psql
import pandas as pd

import calendar
import datetime
from datetime import date, timedelta, datetime
from collections import OrderedDict

# custom packages
import utils_postgres

import logging
log_file = 'test_log_'+str(date.today())+'.log'
print('log_file = ',log_file)
logging.basicConfig(filename=log_file, filemode='w',format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)
# pd.set_option('display.precision', 2) # 2 numbers after decimal point

# Contains credentials like DB, email etc.
config_file = 'config.txt'
import psycopg2 as pg




def query_exec(db_tag,sql_str) : #mysql_18_rbl
    '''
    This f() executes query in database with appropriate db_tag and returns query
	result as pandas DataFrame

	TO DO : Need to close db_conn explicity + logging.
	Otherwise may have problem with large loop.
    '''

    fp             = open(config_file,'r')
    config_dict    = json.load(fp)
    db_server_type = config_dict[db_tag]['db_server_type']

    if db_server_type == 'mysql' :
        db_conn = utils_mysql.get_db_conn(config_file,db_tag)
    elif db_server_type == 'postgresql' :
        db_conn = utils_postgres.get_db_conn(config_file,db_tag)
    elif db_server_type == 'oracle' :
        pass ;
    else :
        sys.exit("ERROR!! Database type (postgres/mysql etc) not matched.. exiting..")


	# execute query and return as pandas DataFrame
    df = pd.read_sql_query(sql_str,con=db_conn)
    res_row_count, res_col_count = df.shape[0], df.shape[1]
    logging.info(sql_str)
    return df, res_row_count
    ###--- END OF query_exec() ---###





if __name__ == '__main__' :

    # postgres
    db_tag = 'postgres_test' # or 'postgres_test'
    sql_str = "select * from product"

    res_df, row_count = query_exec(db_tag, sql_str)
    #print (row_count)

    if row_count > 0 :
        print (res_df)
        res_df.to_excel('t11.xlsx', index=False)
    else :
        print ('Result row_count = 0')
    # writes df to excel file
    logging.info("Process done")
