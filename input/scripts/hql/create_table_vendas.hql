--Script para criacao das tabelas no banco de dados (HIVE)
--TAB_RAW_VENDA="vendas" | TABELA_VENDA="tbl_vendas"

-- Tabela Externa 
CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso_stg.vendas (
	ActualDeliveryDate string,	
	CustomerKey string,	
	DateKey string,		
	DiscountAmount string,	
	InvoiceDate string,	
	InvoiceNumber string,	
	ItemClass string,	
	ItemNumber string,	
	Item string,			
	LineNumber string,	
	ListPrice string,	
	OrderNumber string,	
	PromisedDeliveryDate string,	
	SalesAmount string,	
	SalesAmountBasedonListPrice string,	
	SalesCostAmount string,
	SalesMarginAmount string,	
	SalesPrice string,	
	SalesQuantity string,	
	SalesRep string,	
	U_M string,
	col_extra_1 string,	
	col_extra_2 string, 
	col_extra_3 string, 
	col_extra_4 string, 
	col_extra_5 string, 
	col_extra_6 string	
)
COMMENT 'Tabela de vendas'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/vendas/'
TBLPROPERTIES ("skip.header.line.count"="1");


-- Tabela Gerenciada particionada 
CREATE TABLE IF NOT EXISTS desafio_curso.tbl_vendas(
	ActualDeliveryDate string,	
	CustomerKey string,	
	DateKey string,		
	DiscountAmount string,	
	InvoiceDate string,	
	InvoiceNumber string,	
	ItemClass string,	
	ItemNumber string,	
	Item string,			
	LineNumber string,	
	ListPrice string,	
	OrderNumber string,	
	PromisedDeliveryDate string,	
	SalesAmount string,	
	SalesAmountBasedonListPrice string,	
	SalesCostAmount string,
	SalesMarginAmount string,	
	SalesPrice string,	
	SalesQuantity string,	
	SalesRep string,	
	U_M string,	
	col_extra_1 string,	
	col_extra_2 string, 
	col_extra_3 string, 
	col_extra_4 string, 
	col_extra_5 string, 
	col_extra_6 string
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
  desafio_curso.tbl_vendas
PARTITION(DT_FOTO)
SELECT
	ActualDeliveryDate string,	
	CustomerKey string,	
	DateKey string,		
	DiscountAmount string,	
	InvoiceDate string,	
	InvoiceNumber string,	
	ItemClass string,	
	ItemNumber string,	
	Item string,			
	LineNumber string,	
	ListPrice string,	
	OrderNumber string,	
	PromisedDeliveryDate string,	
	SalesAmount string,	
	SalesAmountBasedonListPrice string,	
	SalesCostAmount string,
	SalesMarginAmount string,	
	SalesPrice string,	
	SalesQuantity string,	
	SalesRep string,	
	U_M string,
	col_extra_1 string,	
	col_extra_2 string, 
	col_extra_3 string, 
	col_extra_4 string, 
	col_extra_5 string, 
	col_extra_6 string,
    current_date as DT_FOTO
FROM desafio_curso_stg.vendas
;
