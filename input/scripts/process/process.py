from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql import functions as f
import os
import re

spark = SparkSession.builder.master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()

# Criando dataframes do Hive
df_cliente = spark.sql("select * from desafio_curso_stg.tbl_clientes")
df_endereco = spark.sql("select * from desafio_curso_stg.tbl_endereco")
df_regiao = spark.sql("select * from desafio_curso_stg.tbl_regiao")
df_divisao = spark.sql("select * from desafio_curso_stg.tbl_divisao")
df_venda = spark.sql("select * from desafio_curso_stg.tbl_vendas")


# Espaço para tratar e juntar os campos e a criação do modelo dimensional
df_cliente.createOrReplaceTempView('cliente')
df_endereco.createOrReplaceTempView('endereco')
df_regiao.createOrReplaceTempView('regiao')
df_divisao.createOrReplaceTempView('divisao')
df_venda.createOrReplaceTempView('venda')


sql = '''
select 
    #--venda--
    vd.CustomerKey      id_cliente_venda,    #PK
    vd.InvoiceNumber    id_fatura_venda,     #PK
    vd.OrderNumber      id_ordem_venda,      #PK
    ItemNumber	        id_item_venda,
    Item                ds_item_venda, 
    vd.SalesQuantity    qt_unidade_vendida,
    vd.SalesPrice       vr_unidade_venda,     --#valor de venda por unidade
    vd.SalesAmount      vr_unidade_somada,    --#valor de venda soma unidade
    vd.SalesCostAmount  vr_custo_venda,
    vd.DiscountAmount   vr_desconto,
    SalesAmountBasedListPrice vr_venda_total, --#valor venda total final acumulado    
    vd.InvoiceDate      dt_venda,
    #--cliente--        --#cl.CustomerKey <-> vd.CustomerKey
    cl.CustomerKey      id_cliente,     
    cl.Customer         nm_cliente,
    #--endereco         --#ed.AddressNumber <-> cl.AddressNumber
    ed.AddressNumber    id_endereco,   
    ed.City             nm_cidade, 
    ed.State            uf_estado,    
    ed.Country          sg_pais,
    #--regiao           --#rg.RegionCode <-> cl.RegionCode
    rg.RegionCode       id_regiao,     
    rg.RegionName       ds_regiao,
    #--divisao          --#dv.Division <-> cl.Division
    dv.Division         id_divisao,    
    dv.DivisionName     ds_divisao
 from 
    venda    as vd 
 inner join     
    cliente  as cl on cl.CustomerKey = vd.CustomerKey 
inner join 
    endereco as ed on ed.AddressNumber = cl.AddressNumber
left join 
    regiao   as rg on rg.RegionCode = cl.RegionCode
left join 
    divisao  as dv on dv.Division = cl.Division    
'''    


# Criação da STAGE
df_stage = spark.sql(sql)

# Criação dos Campos Calendario (df_stage.invoicedate = dt_venda)
df_stage = (df_stage
            .withColumn('Ano', year(df_stage.dt_venda))
            .withColumn('Mes', month(df_stage.dt_venda))
            .withColumn('Dia', dayofmonth(df_stage.dt_venda))
            .withColumn('Trimestre', quarter(df_stage.dt_venda))
           )

# Criação das Chaves do Modelo
df_stage = df_stage.withColumn("DW_CLIENTE", sha2(concat_ws("", df_stage.id_cliente, df_stage.nm_cliente), 256))
df_stage = df_stage.withColumn("DW_TEMPO", sha2(concat_ws("", df_stage.dt_venda, df_stage.Ano, df_stage.Mes, df_stage.Dia), 256))
df_stage = df_stage.withColumn("DW_LOCALIDADE", sha2(concat_ws("", id_endereco, df_stage.ds_cidade, df_stage.uf_estado, df_stage.ds_pais ), 256))
#df_stage = df_stage.withColumn("DW_LOCALIDADE", sha2(concat_ws("", df_stage.id_estado, df_stage.id_cidade, df_stage.ds_estado, df_stage.ds_cidade), 256))

df_stage.createOrReplaceTempView('stage')

#Criando a dimensão Cliente
dim_cliente = spark.sql('''
    SELECT DISTINCT
        DW_CLIENTE,
        id_cliente,
        nm_cliente
    FROM stage    
''')


#Criando a dimensão Tempo
dim_tempo = spark.sql('''
    SELECT DISTINCT
        DW_TEMPO,
        dt_venda,
        Ano,
        Mes,
        Dia
    FROM stage    
''')

#Criando a dimensão Localidade
dim_localidade = spark.sql('''
    SELECT DISTINCT
        DW_LOCALIDADE,
        id_endereco,
        ds_cidade,
        uf_estado,
        ds_pais        
    FROM stage    
''')

#Criando a Fato Vendas
ft_vendas = spark.sql('''
    SELECT        
        DW_CLIENTE,
        DW_TEMPO,
        DW_LOCALIDADE,        
        sum(vr_venda) as vr_total_venda
    FROM stage
    group by 
        DW_CLIENTE,
        DW_TEMPO,
        DW_LOCALIDADE       
''')

# função para salvar os dados
def salvar_df(df, file):
    output = "/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /gold/"+file+".csv"
    print(rename)
    
    
    df.coalesce(1).write\
        .format("csv")\
        .option("header", True)\
        .option("delimiter", ";")\
        .mode("overwrite")\
        .save("/datalake/gold/"+file+"/")

    os.system(erase)
    os.system(rename)

salvar_df(ft_vendas, 'ft_vendas')
salvar_df(dim_cliente, 'dim_cliente')
salvar_df(dim_tempo, 'dim_tempo')
salvar_df(dim_localidade, 'dim_localidade')
