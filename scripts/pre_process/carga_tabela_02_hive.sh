#!/bin/bash

BASEDIR="$( cd "$( dirname "${BASE_SOURCE[0]}" )" && pwd )"
CONFIG="${BASEDIR}/../../config/config.sh"
source "${CONFIG}"

echo "-------------------------------------------------------------"
echo "Iniciando a carga de tabelas do HDFS para o HIVE em ${DATE}"
#TABLES=("clientes" "vendas" "endereco" "regiao" "divisao")

for table in "${TABLES[@]}"
do
  beeline -u jdbc:hive2://localhost:10000 -f ../../hql/create_table_$table.hql
done

echo "Finalizando a carga das tabelas no HIVE em ${DATE}"
echo "-------------------------------------------------------------"


