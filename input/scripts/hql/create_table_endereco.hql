--Script para criacao das tabelas no banco de dados (HIVE)
--TAB_RAW_ENDERECO="endereco" | TABELA_ENDERECO="TBL_ENDERECO"

-- Tabela Externa 
CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso_stg.endereco (
	addressNumber string,	
	city string,	
	country string,	
	customeraddress1 string,	
	customeraddress2 string,	
	customeraddress3 string,	
	customeraddress4 string,	
	state string,	
	zipCode string
)
COMMENT 'Tabela de endereco'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/endereco/'
TBLPROPERTIES ("skip.header.line.count"="1");


-- Tabela Gerenciada particionada 
CREATE TABLE IF NOT EXISTS desafio_curso.tbl_endereco(
	addressNumber string,
	city string,	
	country string,	
	customeraddress1 string,	
	customeraddress2 string,	
	customeraddress3 string,	
	customeraddress4 string,	
	state string,	
	zipCode string
)
PARTITIONED BY (DT_FOTO STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES ('orc.compress'='SNAPPY');

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- Carga 
INSERT OVERWRITE TABLE 
  desafio_curso.tbl_endereco
PARTITION(DT_FOTO)
SELECT
	addressNumber string,	
	city string,	
	country string,	
	customeraddress1 string,	
	customeraddress2 string,	
	customeraddress3 string,	
	customeraddress4 string,	
	state string,	
	zipCode string,
  current_date as DT_FOTO
FROM desafio_curso_stg.endereco
;
