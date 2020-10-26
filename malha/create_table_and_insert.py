#!/usr/bin/env python
# -*- coding: utf-8 -*-

#======================================#
#Author: Jose Carlos da Cruz Barbosa
#Email: jccruz@indracompany.com
#Company: Indra Company
#Number: (83)98650-8210
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
    spark.sql('use {}'.format(database_name))
    tables_df = spark.sql('show tables')
    tables_df.show(truncate=False, n=10)

use_database(database_name)

print('================================ CREATE TABLE {0} 1/4 ================================'.format(objeto))

def show_schema(objeto):
    '''
    This Function that displays the schema of the informed table
    '''
    df = spark.sql('select * from {} limit 1'.format(objeto))
    df.printSchema()

query_fact= os.environ.get('query_create_fact')

def create_table(query, objeto):
    '''
    This Function that creates fact in the target table
    '''
    spark.sql(query)
    show_schema(objeto)

create_table(query, objeto)

print('================================ CREATE TEMPORARY TABLE {0} 2/4 ================================'.format(objeto_tmp))

query_stage= os.environ.get('query_create_stage')
create_table(query_stage, objeto)

def insert_data_in_object_tmp(projeto, objeto, objeto_tmp):
    '''
    This Function Inserting data from The Hdfs Directory
    '''
    insert_data_tmp="""load data inpath '/pre_archive/{0}/{1}' into table {2}""".format(projeto,objeto,objeto_tmp)
    spark.sql(insert_data_tmp)
print('================================ INSERTING  DATA IN TEMPORARY TABLE {0} WITH DT_FOTO {1} 3/4 ================================'.format(objeto_tmp,dt_foto))

insert_data_in_object_tmp(projeto, objeto, objeto_tmp)

query_insert = os.environ.get('insert_data_table')
def insert_data_object(query_insert):
    '''
    This Function Insert data into the final destination table
    '''
    spark.sql(query_insert)
    print(query_insert)

insert_data_object(query_insert)

def drop_table(objeto):
    '''
    This Function that Drop Temporary Table
    '''
    spark.sql('drop table {}'.format(objeto_tmp))

drop_table(objeto_tmp)
 
def count_records_table(objeto):
    '''
    This Function that shows the number of records in the object
    '''
    df = spark.sql('select count(*) as QUANTITY_RECORDS from {}'.format(objeto))
    df.show()


print('================================ QUANTITY OF RECORDS IN THE TABLE {} 4/4 ================================'.format(objeto))
print('*********************************************************************************************************')
count_records_table(objeto)
print('*********************************************************************************************************')
print('================================ FINISHED PROCESS  ================================')
sys.exit()
