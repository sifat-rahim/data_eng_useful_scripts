
import os
import sys
import json
import psycopg2 as pg  # sudo pip3 install psycopg2


def get_db_conn(config_file, db_tag) :
	'''
	This f() triest to connect a database, that has a match with config.txt file and return the conn
	'''

	try :
	    fp          = open(config_file,'r')
	    config_dict = json.load(fp)   #
	    host        = config_dict[db_tag]['host']
	    db_name     = config_dict[db_tag]['db_name']
	    user        = config_dict[db_tag]['user']
	    password    = config_dict[db_tag]['password']
	    db_port     = config_dict[db_tag]['port']

	    # print (host, db_name,user,password,db_port)

	    conn_string = "host='{host}' dbname='{db_name}' user='{user}' password='{password}' port={db_port} ".format(host=host,db_name=db_name,user=user,password=password,db_port=db_port)
	    conn = pg.connect(conn_string)

	except Exception as e:
	    print (str(e))
	    print ("!!ERROR!!, config file load ERROR!!.. exiting program..")
	    sys.exit(0)

	return conn


if __name__ == '__main__':
	#config_file = os.path.join( os.getcwd(), '../config.txt')
	db_conn = get_db_conn(config_file,'postgres_test')
	print(db_conn)

	# check query execution after getting db_conn
	import pandas as pd
	#sql_str = "select count(*) from abc"  # table name : t2
	#sql_str = 'select count(*) from abc'
	df = pd.read_sql(sql_str,con=db_conn)
	print(df)
