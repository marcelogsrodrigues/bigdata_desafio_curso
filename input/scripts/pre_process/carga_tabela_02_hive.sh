#!/bin/bash

BASEDIR="$( cd "$( dirname "${BASE_SOURCE[0]}" )" && pwd )"
CONFIG="${BASEDIR}/../../config/config.sh"
source "${CONFIG}"

#echo "-------------------------------------------------------------"
#echo "Preparando os bancos de dados do HIVE -  ${DATE}"
  #beeline -u jdbc:hive2://localhost:10000 -f ../hql/create_database.hql
  #beeline -u jdbc:hive2://localhost:10000 -e "show databases;"
#echo "-------------------------------------------------------------"

echo "-------------------------------------------------------------"
echo "Iniciando a carga de tabelas do HDFS para o HIVE em ${DATE}"
#TABLES=("clientes" "vendas" "endereco" "regiao" "divisao")

for table in "${TABLES[@]}"
do
 beeline -u jdbc:hive2://localhost:10000 -f ../hql/create_table_$table.hql
done

#teste
#beeline -u jdbc:hive2://localhost:10000 -e "show databases;"
#beeline -u jdbc:hive2://localhost:10000 -e "show tables in desafio_curso_stg;"
#beeline -u jdbc:hive2://localhost:10000 -e "show tables in desafio_curso;"
#beeline -u jdbc:hive2://localhost:10000 -e "select * from desafio_curso.tbl_endereco limit 10;"
#beeline -u jdbc:hive2://localhost:10000 -e "select * from desafio_curso.tbl_endereco limit 10;"
#beeline -u jdbc:hive2://localhost:10000 -e "select CustomerKey,InvoiceNumber,InvoiceDate,ItemNumber,Item from desafio_curso.tbl_vendas limit 10;"

echo "Finalizando a carga das tabelas no HIVE em ${DATE}"
echo "-------------------------------------------------------------"

		

