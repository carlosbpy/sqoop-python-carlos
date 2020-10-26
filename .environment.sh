#! /bin/bash

#======================================#
#Author: Jose Carlos da Cruz Barbosa
#Linkedin: https://www.linkedin.com/in/carlos-barbosa-046a9716b/
#======================================#


#========================== ORIGEM ==========================#
# Credentials
export username=''
export password=''

# TNS Config
export host=
export port=1521
export protocol=''
export service_name=''
#=============================================================#
# Job && Sqoop
export sqoop_reduce=''
export queue=''

#========================== DESTINO ==========================#
# Name Object
export objeto=''
export database_name=''
export objeto_tmp=''

# Data Foto
export dt_today=$(date +%Y%m%d)

# Hdfs Directory
export pre_archive='pre_archive'
export archive='archive'
export main_stage='main_stage'
export caminho_hdfs_pre_archive=/$pre_archive/$projeto/$objeto
#=============================================================#
