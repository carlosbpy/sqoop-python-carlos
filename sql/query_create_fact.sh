#! /bin/bash

#======================================#
#Author: Jose Carlos da Cruz Barbosa
#Linkedin: https://www.linkedin.com/in/carlos-barbosa-046a9716b/
#======================================#


# Query that creates the target table
export query_create_fact='''
             create table if not exists '$objeto'(
              column1              varchar(30),      
              column2              varchar(30))
              partitioned by (dt_foto string)
              '''
