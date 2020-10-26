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
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import lit

# Declare Variables essentials for the Project
projeto= os.environ.get('projeto')
objeto= os.environ.get('objeto')
objeto_tmp= os.environ.get('objeto_tmp')
database_name= os.environ.get('database_name')
dt_foto= os.environ.get('dt_foto')

def time_start_execution():
        '''
        This Function get Start time the Job
        '''
        time_start_execution.time_format= "%H:%M:%S"
        time_start_execution.start= datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        time_start_execution.t1= datetime.now().strftime("%H:%M:%S")
        return time_start_execution.t1

def time_end_execution():
        '''
        This Function get End time the Job
        '''
        time_end_execution.end= datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        time_end_execution.t2= datetime.now().strftime("%H:%M:%S")
        time_duration = datetime.strptime(time_end_execution.t2, time_start_execution.time_format) - datetime.strptime(time_start_execution.t1, time_start_execution.time_format)
        print('========== EXECUTION TIME {} =========='.format(time_duration))
        
time_start_execution()
def use_database(database_name, objeto, projeto):
    '''
    This Function that uses the informed Database
    '''
    try:
        if database_name is None:
            print('the global DATABASE_NAME environment variable has not been defined or the variable DATABASE_NAME has not been correctly informed.')
        else:
            print('========== Database Found Sucessful {} =========='.format(database_name))
            spark.sql('use {}'.format(database_name))
            if objeto is None:
                print('the global OBJETO environment variable has not been defined or the variable OBJETO has not been correctly informed.')
            else:
                tables_df = spark.sql('select * from {} limit 1'.format(objeto))
                print('========== Table Found Sucessful {} =========='.format(objeto))
                print('========== First Line of The Table {} ========== '.format(objeto))
                tables_df_vertical = tables_df.toPandas()
                print(tables_df_vertical.transpose())
    except (Exception) as e:
        print('The .environment file was not loaded, try the command ==> source .environment')
        return False

use_database(database_name, objeto, projeto)

print('================================ CREATE TABLE {0} 1/4 ================================'.format(objeto))

def show_schema(objeto):
    '''
    This Function that displays the schema of the informed table
    '''
    try:
        if objeto is None:
            print('the global OBJETO environment variable has not been defined or the variable OBJETO has not been correctly informed.')
        else:
            df = spark.sql('describe {}'.format(objeto))
            df.show()
    except (Exception) as e:
        print('The .environment file was not loaded, try the command ==> source .environment')
        return False

query_fact= os.environ.get('query_create_fact')

def create_table(query, objeto):
    '''
    This Function that creates fact in the target table
    '''
    try:
        if query is None:
            print('the global QUERY_CREATE_FACT or QUERY_CREATE_STAGE environment variable has not been defined or the variable QUERY_CREATE_FACT or QUERY_CREATE_STAGE has not been correctly informed.')
        else:
            spark.sql(query)
            show_schema(objeto)
    except (Exception) as e:
        print('The .environment file was not loaded, try the command ==> source .environment')
        return False

create_table(query_fact, objeto)

print('================================ CREATE TEMPORARY TABLE {0} 2/4 ================================'.format(objeto_tmp))

query_stage= os.environ.get('query_create_stage')
create_table(query_stage, objeto)

def insert_data_in_object_tmp(projeto, objeto, objeto_tmp):
    '''
    This Function Inserting data from The Hdfs Directory
    '''
    try:
        if projeto or objeto or objeto_tmp is None:
            print('the global PROJETO, OBJETO and OBJETO_TMP environment variable has not been defined or the variable PROJETO, OBJETO and OBJETO_TMP has not been correctly informed.')
        else:
            insert_data_tmp="""load data inpath '/pre_archive/{0}/{1}' into table {2}""".format(projeto,objeto,objeto_tmp)
            spark.sql(insert_data_tmp)
    except (Exception) as e:
        print('The .environment file was not loaded, try the command ==> source .environment')
        return False

print('================================ INSERTING  DATA IN TEMPORARY TABLE {0} WITH DT_FOTO {1} 3/4 ================================'.format(objeto_tmp,dt_foto))

insert_data_in_object_tmp(projeto,objeto,objeto_tmp)

query_insert = os.environ.get('insert_data_table')
def insert_data_object(query_insert, projeto):
    '''
    This Function Insert data into the final destination table
    '''
    try:
        if query_insert is None:
            print('the global INSERT_DATA_TABLE environment variable has not been defined or the variable INSERT_DATA_TABLE has not been correctly informed.')
        else:
            spark.sql(query_insert)
    except (Exception) as e:
        print('The .environment file was not loaded, try the command ==> source .environment')
        return False

insert_data_object(query_insert, projeto)

def drop_table(objeto, projeto):
    '''
    This Function that Drop Temporary Table
    '''
    try:
        if objeto is None:
            print('the global OBJETO environment variable has not been defined or the variable OBJETO has not been correctly informed.')
        else:
            spark.sql('drop table {}'.format(objeto))
    except (Exception) as e:
        print('The .environment file was not loaded, try the command ==> source .environment')
        return False

drop_table(objeto_tmp, projeto)
 
def count_records_table_per_partition(objeto, projeto):
    '''
    This Function that shows the number of records in the object
    '''
    try:
        if objeto is None:
            print('the global OBJETO environment variable has not been defined or the variable OBJETO has not been correctly informed.')
        else:
            df = spark.sql('select count(*) as QUANTITY_RECORDS_PER_PARTITION, dt_foto from {} group by dt_foto order by dt_foto desc'.format(objeto))
            df.show()
    except (Exception) as e:
        print('The .environment file was not loaded, try the command ==> source .environment')
        return False


print('================================ QUANTITY OF RECORDS IN THE TABLE PER PARTITION {} 4/4 ================================'.format(objeto))
print('*********************************************************************************************************')
count_records_table_per_partition(objeto, projeto)
print('*********************************************************************************************************')
time_end_execution()
print('================================ FINISHED PROCESS  ================================')
sys.exit()
