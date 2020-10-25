#! /bin/bash

#======================================#
#Author: Jose Carlos da Cruz Barbosa
#Email: jccruz@indracompany.com
#Company: Indra Company
#Number: (83)98650-8210
#======================================#

# Insert data into the final destination table

export insert_data_table='''
                    insert overwrite table '$database'.'$objeto' partition (dt_foto = '$dt_today')
                    select cd_prdt_lgdo,
                    cd_prdt_lgdo_linh,
                    ds_prdt,
                    cd_ordm_nc,
                    cd_item_ordm_nc,
                    in_stts_prdt, 
                    dt_altr_stts_prdt,
                    ds_tipo_baixa,
                    ds_mtvo_baixa,   
                    dt_baixa,
                    dt_abrt_ctco, 
                    dt_abrt_pddo,
                    dt_abrt_optn, 
                    cd_oprd_ctco,     
                    no_oprd_ctco,     
                    cd_oprd_pddo,     
                    no_oprd_pddo,     
                    cd_oprd_optn,     
                    no_oprd_optn,     
                    in_stts_ctco,     
                    dt_altr_stts_ctco,       
                    cd_usro_altr_ctco,
                    cd_oprd_cncl_pddo,
                    no_oprd_cncl_pddo,
                    no_inpt_nome_de,  
                    in_stts_pddo,     
                    dt_ultm_atlz_pddo,       
                    re_usro_altr_pddo,
                    in_stts_optn 
                    from '$database'.'$objeto_tmp'
                  '''
