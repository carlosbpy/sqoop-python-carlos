#! /bin/bash

#======================================#
#Author: Jose Carlos da Cruz Barbosa
#Linkedin: https://www.linkedin.com/in/carlos-barbosa-046a9716b/
#======================================#

# Query that creates the temporary table

export query_create_stage='''
                 create table if not exists '$objeto_tmp'(
                  column1              varchar(30),      
                  column2              varchar(30))
                  row format delimited fields terminated by ","
		          '''
