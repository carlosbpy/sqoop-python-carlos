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
database= os.environ.get('database')
objeto_tmp= os.environ.get('objeto_tmp')
dt_foto= os.environ.get('dt_today')

# Using Database
spark.sql('use {}'.format(database))

print('================================ EXECUTING 1/5 ================================')

# Query that creates the target table
sql_create= os.environ.get('query_create_fato')
print(sql_create)
spark.sql(sql_create)
print('================================ CREATE TABLE {0} 2/5 ================================'.format(objeto))
print(sql_create)

# Query that creates the temporary table
sql_create_tmp= os.environ.get('query_create_stage')

spark.sql(sql_create_tmp)
print('================================ CREATE TEMPORARY TABLE {0} 3/5 ================================'.format(objeto_tmp))
print(sql_create_tmp)

# Inserting data from the HDFS directory
insert_data_tmp="""load data inpath '/pre_archive/{0}/{1}' into table {2}""".format(projeto,objeto,objeto_tmp)
spark.sql(insert_data_tmp)
print('================================ INSERTING  DATA IN TEMPORARY TABLE {0} WITH DT_FOTO {1} 4/5 ================================'.format(objeto_tmp,dt_foto))
print(insert_data_tmp)

# Insert data into the final destination table
insert_data_table = os.environ.get('insert_data_table')
spark.sql(insert_data_table)
print(insert_data_table)

# Drop Temporary Table
spark.sql('drop table {}'.format(objeto_tmp))

# ================== Shows Number of Records ==================#
df = spark.sql('select count(*) as QUANTITY_RECORDS from {}'.format(objeto))
print('================================ QUANTITY OF RECORDS IN THE TABLE {} 5/5 ================================'.format(objeto))
print('*********************************************************************************************************')
df.show()
print('=========================================================================================================')
print('*********************************************************************************************************')
print('================================ FINISHED PROCESS  ======================================================')
sys.exit()
