#!/bin/bash

#Configuracao por data
DATE="$(date --date="-0 day" "+%Y%m%d")"
PARTICAO="$(date --date="-0 day" "+%Y%m%d")"

##Configuracao datalake
#Caminho datalake
HDFS_DIR_RAW="/datalake/raw"

#Tabelas(raw)
#TABLES=("divisao" "regiao" "clientes" "endereco" "vendas")
TABLES=("vendas")

TAB_RAW_CLIENTE="clientes"
TAB_RAW_VENDA="vendas"
TAB_RAW_ENDERECO="endereco"
TAB_RAW_REGIAO="regiao"
TAB_RAW_DIVISAO="divisao"


#Database
TARGET_STAGE_DATABASE="desafio_curso_stg"
TARGET_DATABASE="desafio_curso"

#Tabelas(hive)
TABELA_CLIENTE="tbl_clientes"
TABELA_VENDA="tbl_vendas"
TABELA_ENDERECO="tbl_endereco"
TABELA_REGIAO="tbl_regiao"
TABELA_DIVISAO="tbl_divisao"

        
        
