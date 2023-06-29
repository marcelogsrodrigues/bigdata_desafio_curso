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

# Criando dataframe divisao
df_divisao = spark.sql("select * from desafio_curso.tbl_divisao")

# Criando dataframe regiao
df_regiao = spark.sql("select * from desafio_curso.tbl_regiao")

# Criando dataframe cliente
df_cliente = spark.sql("select * from desafio_curso.tbl_clientes")

# Criando dataframe endereco
df_endereco = spark.sql("select * from desafio_curso.tbl_endereco")

# Criando dataframe vendas
df_venda = spark.sql("select * from desafio_curso.tbl_vendas")

#verifica estrutura
df_cliente.printSchema()
df_venda.printSchema()   

#teste
df_venda.count()


# Preparar criacao do modelo dimensional
df_divisao.createOrReplaceTempView('divisao')
df_regiao.createOrReplaceTempView('regiao')
df_cliente.createOrReplaceTempView('cliente')
df_endereco.createOrReplaceTempView('endereco')
df_venda.createOrReplaceTempView('venda')


#Esboco do modelo
sql = '''
select 
    --#--venda--
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
    --#--cliente--        --#cl.CustomerKey <-> vd.CustomerKey
    cl.CustomerKey      id_cliente,     
    cl.Customer         nm_cliente,
    --#--endereco         --#ed.AddressNumber <-> cl.AddressNumber
    ed.AddressNumber    id_endereco,   
    ed.City             nm_cidade, 
    ed.State            uf_estado,    
    ed.Country          sg_pais,
    --#--regiao           --#rg.RegionCode <-> cl.RegionCode
    rg.RegionCode       id_regiao,     
    rg.RegionName       ds_regiao,
    --#--divisao          --#dv.Division <-> cl.Division
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

#Query final
sql = '''
select distinct
    vd.customerkey      id_cliente_venda,    
    vd.invoicenumber    id_fatura_venda,    
    vd.ordernumber      id_ordem_venda,      
    vd.itemnumber       id_item_venda,
    vd.item             ds_item_venda, 
    vd.salesquantity    qt_unidade_vendida,
    vd.salesprice       vr_unidade_venda,     
    vd.salesamount      vr_unidade_somada,    
    vd.salescostamount  vr_custo_venda,
    coalesce(vd.discountamount,0) vr_desconto,
    vd.salesamountbasedonlistprice vr_venda_total,     
    vd.invoicedate dt_venda,
    cl.customerkey id_cliente,     
    cl.customername nm_cliente,
    ed.addressnumber id_endereco,   
    coalesce(ed.city, 'Nao Informado') nm_cidade, 
    coalesce(ed.state, 'Nao Informado') uf_estado,    
    coalesce(ed.country, 'Nao Informado') sg_pais,
    coalesce(rg.regiaocode, 0) id_regiao,     
    coalesce(rg.regiaoname'Nao Informado') ds_regiao,
    coalesce(dv.divisao,0) id_divisao,    
    coalesce(dv.divisaoname, 'Nao Informado')       ds_divisao
from 
    venda as vd 
inner join     
    cliente as cl on cl.customerkey == vd.customerkey
inner join 
    endereco as ed on ed.addressnumber == cl.addressnumber
left join 
    divisao as dv on dv.divisao == cl.division  
left join 
    regiao as rg on rg.regiaocode == cl.regioncode     
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
df_stage = df_stage.withColumn("DW_LOCALIDADE", sha2(concat_ws("", df_stage.id_endereco, df_stage.nm_cidade, df_stage.uf_estado, df_stage.sg_pais ), 256))

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
        nm_cidade,
        uf_estado,
        sg_pais        
    FROM stage    
''')

#Criando a Fato Vendas
ft_vendas = spark.sql('''
    SELECT        
        DW_CLIENTE,
        DW_TEMPO,
        DW_LOCALIDADE,        
        sum(vr_venda_total) as vr_total_venda
    FROM stage
    group by 
        DW_CLIENTE,
        DW_TEMPO,
        DW_LOCALIDADE       
''')

# função para salvar os dados
def salvar_df(df, file):
    output = "/input/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /input/gold/"+file+".csv"
    print(rename)
    
    
    df.coalesce(1).write\
        .format("csv")\
        .option("header", True)\
        .option("delimiter", ";")\
        .mode("overwrite")\
        .save("/datalake/gold/"+file+"/")

    os.system(erase)
    os.system(rename)

# Executar funcao para exportar os dados para pasta /gold
salvar_df(ft_vendas, 'ft_vendas')
salvar_df(dim_cliente, 'dim_cliente')
salvar_df(dim_tempo, 'dim_tempo')
salvar_df(dim_localidade, 'dim_localidade')
