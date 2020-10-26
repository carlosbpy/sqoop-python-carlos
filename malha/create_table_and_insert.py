#!/usr/bin/env python
# -*- coding: utf-8 -*-

#======================================#
#Author: Jose Carlos da Cruz Barbosa
#======================================#


# Import essential Libraries for the project
import os, sys
import pyspark
from pyspark.sql.functions import lit

# Target Database name && Name Project && Name Object 

projeto= os.environ.get('projeto')
objeto= os.environ.get('objeto')
objeto_tmp= os.environ.get('objeto_tmp')
dt_foto= os.environ.get('dt_today')


database_name= os.environ.get('database_name')

def use_database(database_name):
    '''
    This Function that uses the informed Database
    '''
    try:
        spark.sql('use {}'.format(database_name))
        tables_df = spark.sql('show tables')
        tables_df.show(truncate=False, n=10)
    except (Exception) as e:
        print('the global DATABASE_NAME environment variable has not been defined')
        return False

use_database(database_name)

print('================================ CREATE TABLE {0} 1/4 ================================'.format(objeto))

def show_schema(objeto):
    '''
    This Function that displays the schema of the informed table
    '''
    try:
        df = spark.sql('select * from {} limit 1'.format(objeto))
        df.printSchema()
    except (Exception) as e:
        print('the global OBJETO environment variable has not been defined')
        return False

query_fact= os.environ.get('query_create_fact')

def create_table(query, objeto):
    '''
    This Function that creates fact in the target table
    '''
    try:
        spark.sql(query)
        show_schema(objeto)
    except (Exception) as e:
        print('the global QUERY_FACT and OBJETO environment variable has not been defined')
        return False

create_table(query_fact, objeto)

print('================================ CREATE TEMPORARY TABLE {0} 2/4 ================================'.format(objeto_tmp))

query_stage= os.environ.get('query_create_stage')
create_table(query_stage, objeto)

def insert_data_in_object_tmp():
    '''
    This Function Inserting data from The Hdfs Directory
    '''
    try:
        insert_data_tmp="""load data inpath '/pre_archive/{0}/{1}' into table {2}""".format(projeto,objeto,objeto_tmp)
        spark.sql(insert_data_tmp)
    except (Exception) as e:
        print('the global PROJETO, OBJETO and OBJETO_TMP environment variable has not been defined')
        return False

print('================================ INSERTING  DATA IN TEMPORARY TABLE {0} WITH DT_FOTO {1} 3/4 ================================'.format(objeto_tmp,dt_foto))

insert_data_in_object_tmp()

query_insert = os.environ.get('insert_data_table')
def insert_data_object(query_insert):
    '''
    This Function Insert data into the final destination table
    '''
    try:
        spark.sql(query_insert)
    except (Exception) as e:
        print('the global INSERT_DATA_TABLE environment variable has not been defined')
        return False

insert_data_object(query_insert)

def drop_table(objeto):
    '''
    This Function that Drop Temporary Table
    '''
    try:
        spark.sql('drop table {}'.format(objeto))
    except (Exception) as e:
        print('the global INSERT_DATA_TABLE environment variable has not been defined')
        return False

drop_table(objeto_tmp)
 
def count_records_table_per_partition(objeto):
    '''
    This Function that shows the number of records in the object
    '''
    try:
        df = spark.sql('select count(*) as QUANTITY_RECORDS_PER_PARTITION, dt_foto from {} group by dt_foto order by dt_foto desc'.format(objeto))
        df.show()
    except (Exception) as e:
        print('the global OBJETO environment variable has not been defined')
        return False


print('================================ QUANTITY OF RECORDS IN THE TABLE PER PARTITION {} 4/4 ================================'.format(objeto))
print('*********************************************************************************************************')
count_records_table_per_partition(objeto)
print('*********************************************************************************************************')
print('================================ FINISHED PROCESS  ================================')
sys.exit()
