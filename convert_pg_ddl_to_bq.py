# -*- coding: utf-8 -*-
"""
Created on : 01 Mar, 2022
Primary author: sifatur.rahim@pathao.com

useful link (to execute meta command in postgres):
1. https://github.com/dbcli/pgspecial
2. https://www.pgcli.com/tag/pgspecial.html
"""

import sys
import json
import calendar
from datetime import date, timedelta, datetime
from collections import OrderedDict

from jinja2 import Template
# Library to run postgres meta commands
from pgspecial.main import PGSpecial

# custom module
import utils_postgres

import logging
log_file = 'test_log_'+str(date.today())+'.log'
print('log_file = ',log_file)
logging.basicConfig(filename=log_file, filemode='w',format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)
# pd.set_option('display.precision', 2) # 2 numbers after decimal point

# Contains credentials like DB, email etc.
config_file = '/home/vm-user/python_projects/jomao-scripts/v3/configs/secrete.yaml'


import psycopg2 as pg


# Required info for bigquery
bq_dataset = 'hermes' #'karagar_food'
gcp_project = 'data-cloud-production'
# list of postgres table from which we want to created BQ ddl schema
#pg_table_list = ['driver_category','jobs','subzones','zones'] #time_slots
pg_table_list = ['merchants'] #time_slots, 'zones' ,'subzones','time_slot_allotments'

db2fs2bq_template = 'table_db2fs2bq_template_yaml.j2'

def create_bq_ddl(table_name, table_cols) :
    '''
    This f() takes input - table_name, table_cols
    prints the bq_ddl schema
    '''
    bq_dataset  =   'hermes' #'karagar_food'
    gcp_project = 'data-cloud-production'

    print('table_name = ',table_name)
    print( 'Total Cols = ', len(table_cols)  )
    partition_by_created = False # used in BQ table ddl
    start_str = '''CREATE TABLE IF NOT EXISTS `data-cloud-production.{bq_dataset}.{table_name}` ( '''.format(bq_dataset=bq_dataset,table_name=table_name)
    #start_str = f'''CREATE TABLE IF NOT EXISTS data-cloud-production.{bq_dataset}.{table_name} ( '''
    print(start_str)
    #print(table_cols)
    for col in table_cols :
        #print(col)

        col_name = col[0].strip()
        col_type = col[1].strip().lower()
        if 'integer' in col_type or 'bigint' in col_type :  # integer or bigint --> INT
            bq_col_type = 'INT'
        elif 'smallint' in col_type  :  # smallint --> SMALLINT
            bq_col_type = 'SMALLINT'
        elif 'double precision' in col_type  :  # integer or bigint --> INT
            bq_col_type = 'FLOAT64'
        elif 'character varying' in col_type or 'text' in col_type or 'uuid' in col_type or 'json' in col_type  : # 'character varying'/text/uuid/json --> STRING
            bq_col_type = 'STRING'
        elif 'boolean' in col_type : # 'boolean' --> BOOL
            bq_col_type = 'BOOL'
        elif 'timestamp' in col_type : # 'timestamp'  --> TIMESTAMP
            bq_col_type = 'TIMESTAMP'
            if col_name.lower() == 'created_at' :
                partition_by_created = True
        else :
            print('Please add new data type in bq for ', col_type.lower() )

        #print( ',', col_name, col_type, bq_col_type  ) # bq create table - debug mode
        print( ',', col_name, bq_col_type  )            # bq create table - final
    print(')')
    if partition_by_created == True :
        print( 'PARTITION BY DATE(created_at) \n;', '\n-- -----------\n')


def create_db2fs2bq_yaml_config(bq_dataset,table_name) : # example
    '''
    This f() takes input - table_name, table_cols
    prints the bq_ddl schema
    '''
    #f_obj_temp = open (db2fs2bq_template, 'r')
    #db2fs2bq_yaml = f_obj_temp.read()

    with open(db2fs2bq_template,'r') as file_:
        template = Template(file_.read())

    data_db2fs2bq = template.render(bq_dataset=bq_dataset, table_name=table_name)
    print(data_db2fs2bq)

    out_file = table_name+'_db2fs2bq.yaml'
    with open(out_file,'w') as f_obj :
        f_obj.write(data_db2fs2bq)
    '''
    new_config = db2fs2bq_yaml.replace('bq_dataset',bq_dataset)
    new_config = new_config.replace('table_name',table_name)
    print(new_config)
    f_obj_temp.close()
    '''

def convert_pg_ddl_to_bq(db_tag, pg_table_list) : # db_tag = 'karagar_db'
    '''
    This f() creates connection according to provided db_tag and executes meta query
	TO DO : Need to close db_conn explicity + logging.
	Otherwise may have problem with large loop.
    '''

    db_server_type = 'postgresql'
    #fp             = open(config_file,'r')
    #config_dict    = json.load(fp)
    #db_server_type = config_dict[db_tag]['db_server_type'] # postgresql / mysql / oracle

    if db_server_type   == 'postgresql' :
        db_conn = utils_postgres.get_db_conn(config_file,db_tag)
    elif db_server_type == 'mysql' :
        pass #db_conn = utils_mysql.get_db_conn(config_file,db_tag)
    elif db_server_type == 'oracle' :
        pass
    else :
        sys.exit("ERROR!! Database type (postgres/mysql/oracle etc) not matched.. exiting..")

    # execute query and return as pandas DataFrame
    pgs = PGSpecial()
    cur = db_conn.cursor()

    for pg_table_name in pg_table_list :
        sql_meta_str = '''\\d {table_name}'''.format(table_name=pg_table_name)
        # print(sql_meta_str)
        for title, rows, headers, status in pgs.execute(cur, sql_meta_str):
            #print ('title = ', title)
            #print( 'headers = ', headers )
            #print ('rows = ', rows)
            pass

        create_bq_ddl(table_name=pg_table_name, table_cols = rows)
        create_db2fs2bq_yaml_config(bq_dataset=bq_dataset,table_name=pg_table_name)

    #logging.info(sql_str)
    ###--- END OF query_exec() ---###



if __name__ == '__main__' :

    # postgres
    db_tag = 'hermes' # ''karagar_db' or 'postgres_test'

    convert_pg_ddl_to_bq(db_tag, pg_table_list) # db_tag = 'postgresql'
    '''
    res_df, row_count = query_exec(db_tag, sql_str)
    #print (row_count)

    if row_count > 0 :
        print (res_df)
        #res_df.to_excel('t11.xlsx', index=False)
    else :
        print ('Result row_count = 0')
    # writes df to excel file
    '''
    logging.info("Process done")
