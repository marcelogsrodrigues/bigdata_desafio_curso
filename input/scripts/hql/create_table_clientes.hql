--Script para criacao das tabelas no banco de dados (HIVE)
--TAB_RAW_CLIENTE="clientes" | TABELA_CLIENTE="TBL_CLIENTES"

-- Tabela Externa 
CREATE EXTERNAL TABLE IF NOT EXISTS ${TARGET_DATABASE}.clientes (
	addressnumber string,	
	businessfamily string,	
	businessunit string,	
	customername string,	
	customerkey string,	
	customertype string,	
	division string,	
	linebusiness string,	
	phone string,	
	regioncode string,	
	regionalsalesmgr string,	
	searchtype string
)
COMMENT 'Tabela de cliente'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/clientes/'
TBLPROPERTIES ("skip.header.line.count"="1");


-- Tabela Gerenciada particionada 
CREATE TABLE IF NOT EXISTS ${TARGET_DATABASE}.tbl_clientes(
	addressnumber string,	
	businessfamily string,	
	businessunit string,	
	customername string,	
	customerkey string,	
	customertype string,	
	division string,	
	linebusiness string,	
	phone string,	
	regioncode string,	
	regionalsalesmgr string,	
	searchtype string
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
  ${TARGET_DATABASE}.tbl_clientes
PARTITION(DT_FOTO)
SELECT
	addressnumber string,	
	businessfamily string,	
	businessunit string,	
	customername string,	
	customerkey string,	
	customertype string,	
	division string,	
	linebusiness string,	
	phone string,	
	regioncode string,	
	regionalsalesmgr string,	
	searchtype string,
       ${PARTICAO} as DT_FOTO
FROM ${TARGET_DATABASE}.clientes
;
