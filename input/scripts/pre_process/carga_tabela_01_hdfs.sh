#!/bin/bash

BASEDIR="$( cd "$( dirname "${BASE_SOURCE[0]}" )" && pwd )"
CONFIG="${BASEDIR}/../../config/config.sh"
source "${CONFIG}"

echo "Iniciando a criacao de tabelas no HDFS em ${DATE}"

#criar diretorio datalake
hdfs dfs -mkdir /datalake
hdfs dfs -chmod 777 /datalake

hdfs dfs -mkdir /datalake/raw
hdfs dfs -chmod 777 /datalake/raw

hdfs dfs -mkdir /datalake/silver
hdfs dfs -chmod 777 /datalake/silver

hdfs dfs -mkdir /datalake/gold
hdfs dfs -chmod 777 /datalake/gold

#carga das tabelas no hive
cd /input/raw

for table in "${TABLES[@]}"
do
    echo "tabela $table"    
    
    #carregar no hdfs
    hdfs dfs -mkdir /datalake/raw/$table
    hdfs dfs -chmod 777 /datalake/raw/$table
    hdfs dfs -copyFromLocal $table.csv /datalake/raw/$table

done

echo "Finalizando a criacao em ${DATE}"


