#! /bin/bash

#======================================#
#Author: Jose Carlos da Cruz Barbosa
#Email: jccruz@indracompany.com
#Company: Indra Company
#Number: (83)98650-8210
#======================================#


#========================== ORIGEM ==========================#
# Credentials
export username='BI_READ_DEV'
export password='biread123'

# TNS Config
export host=10.41.4.251
export port=1521
export protocol='TCP'
export service_name='svcpsie8'
#=============================================================#
# Job && Sqoop
export sqoop_reduce='SQOOP_IMP_MIG_CUST_LOC'
export fila='Desenvolvimento'

#========================== DESTINO ==========================#
# Name Object
export objeto='tbgd_tlza_vnda_sb15_carlos'
export database='p_bigd_lza_db'
export objeto_tmp='tbgd_tlza_vnda_sb15_carlos_tmp'

# Data Foto
export dt_today=$(date +%Y%m%d)

# Hdfs Directory
export pre_archive='pre_archive'
export archive='archive'
export main_stage='main_stage'
export caminho_hdfs_pre_archive=/$pre_archive/$projeto/$objeto
export caminho_hdfs_archive=/$achive/$projeto/$objeto
export caminho_hdfs_main_stage=/$main_stage/$projeto/$objeto
#=============================================================#