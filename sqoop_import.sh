#! /bin/bash

#======================================#
#Author: Jose Carlos da Cruz Barbosa
#Email: jccruz@indracompany.com
#Company: Indra Company
#Number: (83)98650-8210
#======================================#

source /opt/ingestao/automatizador/projetos/$projeto/.environment_siebel15.sh

# Remove pre_archive directory
remove_hdfs='hdfs dfs -rm -r '$caminho_hdfs_pre_archive

$remove_hdfs

sql='''SELECT --COUNT(*)
        DISTINCT
        S_ORDER_ITEM.ASSET_INTEG_ID        AS ID_PRODUTO
        ,S_ORDER_ITEM.ROW_ID               AS ID_CATALOGO_PRODUTO
        ,S_PROD_INT.NAME                   AS DESC_PRODUTO
        ,S_ORDER.X_ORDER_ID_NC             AS ID_ORDEM_NC
        ,S_ORDER_ITEM.PROD_OPT2_VAL_CD     AS ID_ITEM_NC
        ,S_ORDER_ITEM.STATUS_CD            AS DS_STATUS_PRODUTO
        ,S_ORDER.X_ORDER_TYPE              AS DS_TIPO_BAIXA
        ,S_ORDER_ITEM.X_CANCEL_REASON_CD   AS DESC_MOTIVO_BAIXA
        ,S_ORDER_ITEM. LAST_UPD            AS DT_ULT_ATUALIZAÇÃO
        ,S_ORDER_ITEM.X_CANCELLATION_DT    AS DT_BAIXA
        ,S_ORDER.CREATED                   AS DT_ABERTURA_COTACAO
        ,NULL                              AS DT_ABERTURA_PEDIDO
        ,NULL                              AS DT_OPORTUNIDADE
        ,S_USER_OPER.LOGIN                 AS DS_RE_OPERADOR_COTACAO
        ,NULL                              AS NM_OPERADOR_COTACAO
        ,NULL                              AS DS_RE_OPERADOR_PEDIDO
        ,NULL                              AS NM_OPERADOR_PEDIDO
        ,NULL                              AS DS_RE_OPERADOR_OPORTUNIDADE
        ,NULL                              AS NM_OPERADOR_OPORTUNIDADE
        ,S_ORDER.STATUS_CD                 AS DS_STATUS_COTACAO
        ,S_ORDER.LAST_UPD                  AS DS_ALTERACAO_COTACAO
        ,S_USER_COT.LOGIN                  AS DS_RE_USER_COTACAO
        ,NULL                              AS DS_RE_USER_OPER_PEDIDO
        ,NULL                              AS NM_OPERADOR_IMPUT
        ,S_USER_INPUT.LOGIN                AS NM_IMPUTADO
        ,NULL                              AS DS_STATUS_PEDIDO
        ,S_ORDER_DTL.X_COMPLETE_DT         AS DT_ULT_STATUS_PEDIDO
        ,NULL                              AS DS_RE_ALT_STATUS_PEDIDO
        ,NULL                              AS DS_STATUS_OPORTUNIDADE
        FROM (SELECT * FROM SIEBEL.S_ORDER WHERE S_ORDER.X_ORDER_ID_NC IS NOT NULL) S_ORDER
        INNER JOIN SIEBEL.S_ORDER_ITEM
        ON (S_ORDER.ROW_ID=S_ORDER_ITEM.ORDER_ID)
        LEFT JOIN SIEBEL.S_PROD_INT
        ON (S_ORDER_ITEM.PROD_ID=S_PROD_INT.ROW_ID)
        LEFT JOIN SIEBEL.S_ORDER_DTL
        ON (S_ORDER_ITEM.ORDER_ID = S_ORDER_DTL.ROW_ID)
        LEFT JOIN SIEBEL.S_USER S_USER_OPER
        ON (S_ORDER.X_SALES_LOGIN_ID=S_USER_OPER.PAR_ROW_ID)
        LEFT JOIN SIEBEL.S_USER S_USER_COT
        ON (S_ORDER.CREATED_BY=S_USER_COT.PAR_ROW_ID)
        LEFT JOIN SIEBEL.S_ORDER_POSTN
        ON (S_ORDER.ROW_ID = S_ORDER_POSTN.ORDER_ID AND S_ORDER_POSTN.POSTN_ID = S_ORDER.PR_POSTN_ID)
        LEFT JOIN SIEBEL.S_PARTY
        ON (S_ORDER.PR_POSTN_ID=S_PARTY.PARTY_UID )
        LEFT JOIN SIEBEL.S_POSTN
        ON (S_PARTY.PARTY_UID=S_POSTN.ROW_ID )
        LEFT JOIN SIEBEL.S_USER S_USER_INPUT
        ON (S_POSTN.PR_EMP_ID=S_USER_INPUT.PAR_ROW_ID ) 
	WHERE $CONDITIONS
	'''

sqoop import -D mapreduce.job.name="$sqoop_reduce" -D mapreduce.job.queuename=$fila \
-D tez.queue.name=$fila \
-D mapreduce.child.java.opts="\-Djava.security.egd=file:/dev/urandom" \
--connect jdbc:oracle:thin:@"(DESCRIPTION=(CONNECT_DATA=(SERVICE_NAME=$service_name))(ADDRESS=(PROTOCOL=$protocol)(HOST=$host)(PORT=$port)))" \
--username "$username" --password "$password" \
--query "$sql" --split-by 1 \
--target-dir $caminho_hdfs_pre_archive

# hdfs dfs -rm -r $caminho_hdfs_main_stage
# hdfs dfs -rm -r $caminho_hdfs_archive

# hdfs dfs -mkdir  $caminho_hdfs_main_stage
# hdfs dfs -mkdir  $caminho_hdfs_archive

# hdfs dfs -copyFromLocal $caminho_hdfs_pre_archive $caminho_hdfs_main_stage
# hdfs dfs -copyFromLocal $caminho_hdfs_pre_archive $caminho_hdfs_archive

source /opt/ingestao/automatizador/projetos/$projeto/.ipythonstart_siebel15

