#!/bin/bash

BASEDIR="$( cd "$( dirname "${BASE_SOURCE[0]}" )" && pwd )"
CONFIG="${BASEDIR}/../../config/config.sh"
source "${CONFIG}"

echo "Iniciando a criacao de tabelas no HDFS em ${DATE}"

for table in "${TABLES[@]}"
do
    echo "tabela $table"
    cd ../../raw
    
    #carregar no hdfs
    hdfs dfs -mkdir /datalake/raw/$table
    hdfs dfs -chmod 777 /datalake/raw/$table
    hdfs dfs -copyFromLocal $table.csv /datalake/raw/$table

done

echo "Finalizando a criacao em ${DATE}"


