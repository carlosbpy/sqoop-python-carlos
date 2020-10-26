#! /bin/bash

#======================================#
#Author: Jose Carlos da Cruz Barbosa
#Linkedin: https://www.linkedin.com/in/carlos-barbosa-046a9716b/
#======================================#

source /opt/ingestao/automatizador/projetos/$projeto/.environment.sh

# Remove pre_archive directory
remove_hdfs='hdfs dfs -rm -r '$caminho_hdfs_pre_archive

$remove_hdfs

sql='''Select Here
	WHERE $CONDITIONS
	'''

sqoop import -D mapreduce.job.name="$sqoop_reduce" -D mapreduce.job.queuename=$fila \
-D tez.queue.name=$fila \
-D mapreduce.child.java.opts="\-Djava.security.egd=file:/dev/urandom" \
--connect jdbc:oracle:thin:@"(DESCRIPTION=(CONNECT_DATA=(SERVICE_NAME=$service_name))(ADDRESS=(PROTOCOL=$protocol)(HOST=$host)(PORT=$port)))" \
--username "$username" --password "$password" \
--query "$sql" --split-by 1 \
--target-dir $caminho_hdfs_pre_archive

source /opt/ingestao/automatizador/projetos/$projeto/.ipythonstart

