DESAFIO BIG DATA
---
Área de negócio: Vendas.
Proposta: Prover dados que permita a geração de relatorios gerencias.

Escopo: Ingestão de dados, processamento, modelagem dimensional, geração de relatório para analise.

---

## Modelagem relacional: Vendas, clientes, endereço, região, divisão.
![desafio_curso_modelo_dados_relacional](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/b842ad6e-cba5-4e30-81a5-a80f73fe7b76)


## Modelagem dimensional: Vendas, Clientes, Tempo e Locadade.
![desafio_curso_modelo_dados_dimensional](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/f0084192-dfd8-4a83-aee0-f28d81747888)


## Ferramentas e tecnologias utilizadas:
•	Git e Git Hub
•	Docker e Docker Compose
•	Shell Script
•	Python
•	HADOOP
•	HDFS
•	Hive
•	HQL
•	SPARK
•	Jupyter Notebook

![Diagrama_Fluxo_Dados](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/a48a5251-c28c-4a8f-8786-0030a207447d)


## Etapas de desenvolvimento e testes
---
## _Etapa 0: Preparar o ambiente de desenvolvimento_
![docker-desafio-curso-01-preparando-ambiente-2023-06-28 16-06-58](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/d85952ba-147e-46f7-998b-a9e697d6b567)
![docker-desafio-curso-01-preparando-ambiente-2023-06-28 16-05-17](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/545198d7-8314-40a1-916f-6cf301489580)
![docker-desafio-curso-01-preparando-ambiente-2023-06-28 16-13-47](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/95a67f14-9678-42c1-b2a5-28945d297f9b)
![docker-desafio-curso-01-preparando-ambiente-2023-06-28 16-48-20](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/64a2a829-4b39-4181-bbed-d0b70a00cddb)
. Configurando git e docker

## _Etapa 1: Enviar os arquivos para o HDFS_
![docker-desafio-curso-01B-enviar-arquivo-hdfs-2023-06-28 17-43-20](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/7868d90e-c87f-4e00-9deb-890f705b19d4)

.
## _Etapa 2: Criar o banco DESAFIO_CURSO e tabelas no HIVE. Carga com shell script e HQL_
![docker-desafio-curso-02-cargahive-2023-06-28 19-20-28](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/711caea4-9422-45d7-9c1f-ab4d6f1ea056)
![docker-desafio-curso-02-cargahive-2023-06-28 19-52-11](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/7ecbdc9a-9c37-4b85-88e4-f300d395cd77)
![docker-desafio-curso-02-cargahive-OK- 2023-06-28 21-17-20](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/885a3cd9-1f96-4e3f-a323-1e5a8b2be6ed)
![docker-desafio-curso-02-cargahive-OK- 2023-06-28 21-19-20](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/cdc94f8b-a440-4427-84de-b692ae5b2a93)

.
## _Etapa 3: Processar os dados no Spark_
![docker-desafio-curso-03-Processamento-Spark-Jupyter-2023-06-28 22-06-29](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/67c6ce45-2456-4b0d-a020-bfaa87be5cae)
![docker-desafio-curso-03-Processamento-Spark-Jupyter-2023-06-28 23-53-48](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/972b2dd3-a5fd-4335-b332-bde3331b0933)
![docker-desafio-curso-03-Processamento-Spark-Jupyter-2023-06-28 23-55-00](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/d6d01894-776b-4230-8d63-84851d7f9b05)
![docker-desafio-curso-03-Processamento-Spark-Jupyter-2023-06-28 23-56-00](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/4551880d-31f4-497d-8e34-1e7d72e20f7e)

.
## _Etapa 4: Gravar as informações em tabelas dimensionais em formato csv_
![docker-desafio-curso-04-2023-06-29 00-30-50](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/9b0fc512-7508-413c-b6fd-c7d56a232e63)

.
## _Etapa 5: Exportar os dados para a pasta desafio_curso/gold_
![docker-desafio-curso-05- 2023-06-29 00-33-05](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/ffa18664-5353-4de7-889d-dbf8328061a0)

.
## _Etapa 6: Criar gráficos de vendas no PowerBI_
![docker-desafio-curso-06-Power-BI-01](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/a9d5d0af-8c46-4da0-bedb-c3b41ef9bbb0)
.
![docker-desafio-curso-06-Power-BI-02-Relatorio-Vendas-por-Estado](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/5be4d17e-459b-40e1-83b9-cf28c1153853)
![docker-desafio-curso-06-Power-BI-03-Relatorio-Vendas-por-Cliente](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/901cb65c-59b3-4c5d-9592-78788137a804)
![docker-desafio-curso-06-Power-BI-04-Relatorio-Vendas-por-Ano](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/2305fd8c-42bf-462c-941a-4504f8a91d27)
![docker-desafio-curso-06-Power-BI-05-Relatorio-SomaTotalVendas](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/e12c0622-44d0-417d-8504-15a15f38a9d6)
![docker-desafio-curso-06-Power-BI-06-Relatorio-QuantidadeVendas](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/0a920f71-c11a-4d09-86b7-2bd9e584c4d7)

.
---
![Architecture-BronzeSilverGold](https://github.com/marcelogsrodrigues/bigdata_desafio_curso/assets/134144307/f52ebd61-38e7-42fc-bd11-c439012eceb0)
---
