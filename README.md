
# Projeto Siebel15 | Ingestão TBGD_TLZA_VNDA_SB15

### ======================================
### - Author: Jose Carlos da Cruz Barbosa
### - Email: jccruz@indracompany.com
### - Company: Indra Company
### - Number: (83)98650-8210
### ======================================

### Objetivo:
  - [x] Parametrizar o TNS para conexão com Banco de Dados Oracle
  - [x] Montar Shell com command Sqoop import do oracle;
  - [x] Gravar no HDFS no diretório /pre_archive/projeto/objeto;
  - [x] Criar Tabela de Destino via Spark Particionada pela Data Foto;
  - [x] Criar Tabela Temporária com Delimitador ',';
  - [x] Realizar Insert à partir do Diretório no Hdfs para tabela temporária;
  - [x] Realizar Insert na Tabela de Destino à partir da Tabela temporária
  - [x] Dropar Tabela temporária
