#!/usr/bin/env bash

#======================================#
#Author: Jose Carlos da Cruz Barbosa
#Email: jccruz@indracompany.com
#Company: Indra Company
#Number: (83)98650-8210
#======================================#

set -x

if [ $# -ne 5 ]; then
        echo -e $(date +"%Y-%m-%d %H:%M:%S,%3N")" - \033[1;31mERROR\033[0m - $(basename $0): Numero de parametros invalido."
        echo -e $(date +"%Y-%m-%d %H:%M:%S,%3N")" - \033[1;31mERROR\033[0m - Parametros obrigatorios:\n$(basename $0) <nivel de log> <caminho absoluto do log> <projeto> <banco dados fato>"
        exit 134
fi

source "$DIR_INGESTAO_VIVO_TRIAD/projetos/legado/legado_big_data.properties"
if [ $? -ne 0 ]; then
        echo -e $(date +"%Y-%m-%d %H:%M:%S,%3N")" - \033[1;31mERROR\033[0m - Nao foi possivel carregar o properties do legado big data"
        echo -e $(date +"%Y-%m-%d %H:%M:%S,%3N")" - \033[1;31mERROR\033[0m - Nao foi possivel carregar o properties do legado big data" >> $2
        exit 134
fi

source "$DIR_INGESTAO_VIVO_TRIAD/projetos/legado/rotinas_auxiliares.sh"
if [ $? -ne 0 ]; then
        echo -e $(date +"%Y-%m-%d %H:%M:%S,%3N")" - \033[1;31mERROR\033[0m - Nao foi possivel carregar o script de rotinas auxiliares"
        echo -e $(date +"%Y-%m-%d %H:%M:%S,%3N")" - \033[1;31mERROR\033[0m - Nao foi possivel carregar o script de rotinas auxiliares" >> $2
        exit 134
fi

inicializar_log $1 $2
if [ $? -ne 0 ]; then
        logar_erro "Erro ao inicializar log do script da chamada configuravel."
        exit 134
fi

logar $INFO "INICIANDO PROCESSO DE INGESTAO PARA O HIVE"

export projeto='siebel15_carlos'

source /opt/ingestao/automatizador/projetos/$projeto/.environment_siebel15.sh
source /opt/ingestao/automatizador/projetos/$projeto/sql/insert_data_table.sh
source /opt/ingestao/automatizador/projetos/$projeto/sql/query_create_fato.sh
source /opt/ingestao/automatizador/projetos/$projeto/sql/query_create_stage.sh

cd /opt/ingestao/automatizador/projetos/$projeto/ && chmod +x sqoop_import.sh && ./sqoop_import.sh

rm /opt/ingestao/automatizador/projetos/$projeto/QueryResult.java