#!/bin/bash

#Configuracao por data
DATE="$(date --date="-0 day" "+%Y%m%d")"
PARTICAO="$(date --date="-0 day" "+%Y%m%d")"

##Configuracao datalake
#Caminho datalake
HDFS_DIR_RAW="/datalake/raw"

#Tabelas(raw)
TABLES=("clientes" "vendas" "endereco" "regiao" "divisao")
TAB_RAW_CLIENTE="clientes"
TAB_RAW_VENDA="vendas"
TAB_RAW_ENDERECO="endereco"
TAB_RAW_REGIAO="regiao"
TAB_RAW_DIVISAO="divisao"


##Configuracao hive
#Database
TARGET_DATABASE="DESAFIO_CURSO"

#Tabelas(hive)
TABELA_CLIENTE="TBL_CLIENTES"
TABELA_VENDA="TBL_VENDAS"
TABELA_ENDERECO="TBL_ENDERECO"
TABELA_REGIAO="TBL_REGIAO"
TABELA_DIVISAO="TBL_DIVISAO"

        
        
