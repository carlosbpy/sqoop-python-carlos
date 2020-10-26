#!/usr/bin/env bash

#======================================#
#Author: Jose Carlos da Cruz Barbosa
#Linkedin: https://www.linkedin.com/in/carlos-barbosa-046a9716b/
#======================================#

export projeto=''

source /opt/ingestao/automatizador/projetos/$projeto/.environment_siebel15.sh
source /opt/ingestao/automatizador/projetos/$projeto/sql/insert_data_table.sh
source /opt/ingestao/automatizador/projetos/$projeto/sql/query_create_fact.sh
source /opt/ingestao/automatizador/projetos/$projeto/sql/query_create_stage.sh

cd /opt/ingestao/automatizador/projetos/$projeto/ && chmod +x sqoop_import.sh && ./sqoop_import.sh

rm /opt/ingestao/automatizador/projetos/$projeto/QueryResult.java
