--Script para criacao das tabelas no banco de dados (HIVE)
--TAB_RAW_REGIAO="regiao"| TABELA_REGIAO="tbl_regiao"

-- Tabela Externa 
CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso_stg.regiao (
	regiaocode string,	
	regiaoname string
)
COMMENT 'Tabela de regiao'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/regiao/'
TBLPROPERTIES ("skip.header.line.count"="1");


-- Tabela Gerenciada particionada 
CREATE TABLE IF NOT EXISTS desafio_curso.tbl_regiao(
	regiaocode string,	
	regiaoname string
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
  desafio_curso.tbl_regiao
PARTITION(DT_FOTO)
SELECT
  regiaocode string,	
  regiaoname string,
  current_date as DT_FOTO
FROM desafio_curso_stg.regiao
;
