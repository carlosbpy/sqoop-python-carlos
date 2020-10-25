#! /bin/bash

#======================================#
#Author: Jose Carlos da Cruz Barbosa
#Email: jccruz@indracompany.com
#Company: Indra Company
#Number: (83)98650-8210
#======================================#

# Query that creates the temporary table

export query_create_stage='''
                 create table if not exists '$objeto_tmp'(
                  cd_prdt_lgdo              varchar(30),      
                  cd_prdt_lgdo_linh         varchar(30),      
                  ds_prdt                   varchar(255),      
                  cd_ordm_nc                varchar(100),      
                  cd_item_ordm_nc           varchar(100),      
                  in_stts_prdt              varchar(30),      
                  dt_altr_stts_prdt         timestamp,        
                  ds_tipo_baixa             varchar(50),       
                  ds_mtvo_baixa             varchar(50),       
                  dt_baixa                  timestamp,         
                  dt_abrt_ctco              timestamp,         
                  dt_abrt_pddo              timestamp,         
                  dt_abrt_optn              timestamp,         
                  cd_oprd_ctco              varchar(20),       
                  no_oprd_ctco              varchar(120),      
                  cd_oprd_pddo              varchar(20),       
                  no_oprd_pddo              varchar(120),      
                  cd_oprd_optn              varchar(20),       
                  no_oprd_optn              varchar(120),      
                  in_stts_ctco              varchar(30),       
                  dt_altr_stts_ctco         timestamp,         
                  cd_usro_altr_ctco         varchar(20),       
                  cd_oprd_cncl_pddo         varchar(20),       
                  no_oprd_cncl_pddo         varchar(120),      
                  no_inpt_nome_de           varchar(120),      
                  in_stts_pddo              varchar(30),       
                  dt_ultm_atlz_pddo         timestamp,         
                  re_usro_altr_pddo         varchar(20),       
                  in_stts_optn              varchar(30))
                  row format delimited fields terminated by ","
		          '''
