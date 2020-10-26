#! /bin/bash

#======================================#
#Author: Jose Carlos da Cruz Barbosa
#Linkedin: https://www.linkedin.com/in/carlos-barbosa-046a9716b/
#======================================#

# Insert data into the final destination table

export insert_data_table='''
                    insert overwrite table '$database_name'.'$objeto' partition (dt_foto = '$dt_today')
                    select column1,
                    column2 
                    from '$database_name'.'$objeto_tmp'
                  '''
