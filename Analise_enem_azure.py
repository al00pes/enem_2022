# Databricks notebook source
# DBTITLE 1,Importando as bibliotecas
from pyspark.sql.functions import *
from pyspark.sql.functions import regexp_replace

# COMMAND ----------

# DBTITLE 1,Ponto de montagem camada bronze
dbutils.fs.mount(
    source = "wasbs://bronze@storageenem2000.blob.core.windows.net",
mount_point = "/mnt/storageenem2000/bronze",
extra_configs = {"fs.azure.account.key.storageenem2000.blob.core.windows.net":"ZP80pIRUCphDD5JuILTx1cIL6+yyLJXizbNHuLaylDipkV8EC5CVEGJzQhADqfKCSHVHCOTdOMpH+AStRHWSJg=="}

)

# COMMAND ----------

# DBTITLE 1,Ponto de montagem camada Silver
dbutils.fs.mount(
    source = "wasbs://silver@storageenem2000.blob.core.windows.net",
mount_point = "/mnt/storageenem2000/silver",
extra_configs = {"fs.azure.account.key.storageenem2000.blob.core.windows.net":"ZP80pIRUCphDD5JuILTx1cIL6+yyLJXizbNHuLaylDipkV8EC5CVEGJzQhADqfKCSHVHCOTdOMpH+AStRHWSJg=="}

)

# COMMAND ----------

# DBTITLE 1,Ponto de montagem camada Gold
dbutils.fs.mount(
    source = "wasbs://gold@storageenem2000.blob.core.windows.net",
mount_point = "/mnt/storageenem2000/gold",
extra_configs = {"fs.azure.account.key.storageenem2000.blob.core.windows.net":"ZP80pIRUCphDD5JuILTx1cIL6+yyLJXizbNHuLaylDipkV8EC5CVEGJzQhADqfKCSHVHCOTdOMpH+AStRHWSJg=="}

)

# COMMAND ----------

# DBTITLE 1,Criando um database
spark.sql("CREATE DATABASE IF NOT EXISTS enem")

# COMMAND ----------

# DBTITLE 1,Lendo o arquivo csv da camada bronze
df_enem_2022_bronze = spark.read.format('csv')\
		     .options(header='true', infer_schema='true', delimiter=';')\
		     .load('dbfs:/mnt/storageenem2000/bronze/MICRODADOS_ENEM_2022.csv')
       
df_enem_2021_bronze = spark.read.format('csv')\
				.options(header='true', infer_schema='true', delimiter=';')\
				.load('dbfs:/mnt/storageenem2000/bronze/MICRODADOS_ENEM_2021.csv')
       

# COMMAND ----------

# DBTITLE 1,Instanciando um daframe onde não existem valores nulos na nota da redação
#dataframe enem 2022
df_enem_2022_silver = df_enem_2022_bronze.filter(df_enem_2022_bronze.NU_NOTA_REDACAO.isNotNull())

#dataframe enem 2021
df_enem_2021_silver = df_enem_2021_bronze.filter(df_enem_2021_bronze.NU_NOTA_REDACAO.isNotNull())

# COMMAND ----------

# DBTITLE 1,Criando arquivo parquet
#Criando arquivo parquet no dir
df_enem_2021_silver.write.format('delta')\
    .mode('overwrite')\
    .save('/mnt/storageenem2000/silver/enem_2021')


df_enem_2022_silver.write.format('delta')\
    .mode('overwrite')\
    .save('/mnt/storageenem2000/silver/enem_2022')

# COMMAND ----------

# DBTITLE 1,Criando tabelas no database "enem" 
#Usar database
spark.sql('USE enem')

#Criar tabela enem_2022
spark.sql("""
CREATE TABLE IF NOT EXISTS enem_2022
USING DELTA 
LOCATION '/mnt/storageenem2000/silver/enem_2022'
""")

#Criar tabela enem_2022
spark.sql("""
CREATE TABLE IF NOT EXISTS enem_2021
USING DELTA 
LOCATION '/mnt/storageenem2000/silver/enem_2021'
""")

# COMMAND ----------

#Query

df_resultado = spark.sql('SELECT * FROM enem_2021')

display(df_resultado)

# COMMAND ----------

# DBTITLE 1,Camada Gold
#Camada gold
from pyspark.sql.functions import concat,to_date, year


# COMMAND ----------

# DBTITLE 1,Lendo os arquivo parquet
df_enem_2022_silver = spark.read.format('delta')\
    .load('/mnt/storageenem2000/silver/enem_2022')

df_enem_2021_silver = spark.read.format('delta')\
    .load('/mnt/storageenem2000/silver/enem_2021')

# COMMAND ----------

from pyspark.sql.functions import concat,to_date, year

# Tranformar o type da coluna "NU_ANO"

df_enem_2022_silver = df_enem_2022_silver.withColumn("NU_ANO", to_date(df_enem_2022_silver['NU_ANO']))

# COMMAND ----------

# DBTITLE 1,Renomeando as colunas
df_enem_gold = df_enem_2022_silver.withColumnRenamed("NU_INSCRICAO","INSCRICAO")\
                             .withColumnRenamed("TP_FAIXA_ETARIA","FAIXA_ETARIA")

# COMMAND ----------

df_enem_gold = df_enem_gold.withColumn('YEAR',year(df_enem_gold['NU_ANO']))

# COMMAND ----------

# DBTITLE 1,Modificando o type da coluna
from pyspark.sql.functions import col

df_enem_gold = df_enem_gold.withColumn('NU_ANO',col('NU_ANO').cast("integer"))

# COMMAND ----------

# Gravar os dados no Delta Lake, particionados por 'ano' 
df_enem_gold.write.format("delta")\
    .mode('overwrite')\
    .option("overwriteSchema", "true")\
    .partitionBy("YEAR")\
    .save("/mnt/storageenem2000/gold/enem_full")

   

# COMMAND ----------

# Use enem database

spark.sql('USE enem')

#Create table externa

spark.sql("""
DROP TABLE IF EXISTS enem_gold """)

spark.sql("""
CREATE TABLE IF NOT EXISTS enem_gold
USING DELTA
LOCATION '/mnt/storageenem2000/gold/enem_full'
""")

# COMMAND ----------

# Query

df_enem_gold = spark.sql("SELECT * FROM enem_gold")

display(df_enem_gold)

# COMMAND ----------

# DBTITLE 1,Criando select agregado
#Criar table agregada

from pyspark.sql.functions import count

#Selecionando as colunas que irei trabalhar
select_columns = ["YEAR","FAIXA_ETARIA","TP_SEXO"]

#Criando um dataframe somente com as colunas especificas mencionado acima
df_select_columns_gold = df_enem_gold.select(select_columns)

#agrupando as colunas selecionada e realizando a contagem

grouped_df = df_select_columns_gold.groupBy(select_columns).agg(count("*").alias("count"))

# COMMAND ----------

grouped_df = grouped_df.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^1$","Menor de 17 anos"))

grouped_df = grouped_df.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^2$","17 anos"))

grouped_df = grouped_df.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^3$","18 anos"))

grouped_df = grouped_df.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^4$","19 anos"))

grouped_df = grouped_df.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^5$","20 anos"))

grouped_df = grouped_df.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^6$","21 anos"))

grouped_df = grouped_df.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^7$","22 anos"))

grouped_df = grouped_df.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^8$","23 anos"))

grouped_df = grouped_df.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^9$","24 anos"))

grouped_df = grouped_df.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^10$","25 anos"))

grouped_df = grouped_df.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^11$","Entre 26 e 30 anos"))

grouped_df = grouped_df.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^12$","Entre 31 e 35 anos"))

grouped_df = grouped_df.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^13$","Entre 36 e 40 anos"))

grouped_df = grouped_df.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^14$","Entre 41 e 45 anos"))

grouped_df = grouped_df.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^15$","Entre 46 e 50 anos"))

grouped_df = grouped_df.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^16$","Entre 51 e 55 anos"))

grouped_df = grouped_df.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^17$","Entre 56 e 60 anos"))

grouped_df = grouped_df.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^18$","Entre 61 e 65 anos"))

grouped_df = grouped_df.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^19$","Entre 66 e 70 anos"))

grouped_df = grouped_df.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^20$","Maior 70"))

display(grouped_df)

# COMMAND ----------

# DBTITLE 1,Salvando no conteiner
df_enem_gold.write.format("delta")\
    .mode('overwrite')\
    .option("overwriteSchema", "true")\
    .partitionBy("YEAR")\
    .save("/mnt/storageenem2000/gold/enem_faixa_etaria_agg")


# COMMAND ----------

# DBTITLE 1,Criando uma tabela apartir aqui delta
# Use enem database

spark.sql('USE enem')

#Create table externa

spark.sql("""
DROP TABLE IF EXISTS enem_faixa_etaria_agg """)

spark.sql("""
CREATE TABLE IF NOT EXISTS enem_faixa_etaria_agg
USING DELTA
LOCATION '/mnt/storageenem2000/gold/enem_faixa_etaria_agg'
""")

# COMMAND ----------

#Criar table agregada

from pyspark.sql.functions import count

#Selecionando as colunas que irei trabalhar
select_columns = ["YEAR","FAIXA_ETARIA","TP_SEXO","TP_ST_CONCLUSAO","TP_ESCOLA","NO_MUNICIPIO_PROVA","SG_UF_PROVA"]

#Criando um dataframe somente com as colunas especificas mencionado acima
df_select_columns_gold_2 = df_enem_gold.select(select_columns)

#agrupando as colunas selecionada e realizando a contagem
grouped_df_02 = df_select_columns_gold_2.groupBy(select_columns).agg(count("*").alias("count"))

# COMMAND ----------

display(grouped_df_02)


# COMMAND ----------

# DBTITLE 1,Modificando os nomes das colunas
grouped_df_02 = grouped_df_02.withColumnRenamed("YEAR","ANO")
grouped_df_02 = grouped_df_02.withColumnRenamed("TP_SEXO","SEXO")
grouped_df_02 = grouped_df_02.withColumnRenamed("TP_ST_CONCLUSAO","ANO_CONCLUSAO")
grouped_df_02 = grouped_df_02.withColumnRenamed("TP_ESCOLA","TIPO_ESCOLA")
grouped_df_02 = grouped_df_02.withColumnRenamed("NO_MUNICIPIO_PROVA","MUNICIPIO_PROVA")
grouped_df_02 = grouped_df_02.withColumnRenamed("SG_UF_PROVA","UF_PROVA")
grouped_df_02 = grouped_df_02.withColumnRenamed("count","TOTAL")

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Modificando os valores 
grouped_df_02 = grouped_df_02.withColumn("TIPO_ESCOLA", regexp_replace("TIPO_ESCOLA","^1$","Não Respondeu"))

grouped_df_02 = grouped_df_02.withColumn("TIPO_ESCOLA", regexp_replace("TIPO_ESCOLA","^2$","Pública"))

grouped_df_02 = grouped_df_02.withColumn("TIPO_ESCOLA", regexp_replace("TIPO_ESCOLA","^3$","Privada"))



# COMMAND ----------

#Exibindo o dataframe
display(grouped_df_02)

# COMMAND ----------

display(df_enem_gold)

# COMMAND ----------

select_columns = ["NU_ANO","FAIXA_ETARIA","TP_SEXO","TP_ST_CONCLUSAO","TP_ESCOLA","CO_MUNICIPIO_PROVA","SG_UF_PROVA"]

#Criando um dataframe somente com as colunas especificas mencionado acima
df_select_columns_gold_2 = df_enem_gold.select(select_columns)

#agrupando as colunas selecionada e realizando a contagem
grouped_df_02 = df_select_columns_gold_2.groupBy(select_columns).agg(count("*").alias("count"))

# COMMAND ----------

# DBTITLE 1,Modificando o nome das colunas
select_columns = ['INSCRICAO','NU_NOTA_CN','NU_NOTA_CH','NU_NOTA_LC','NU_NOTA_MT','NU_NOTA_REDACAO',"FAIXA_ETARIA","SG_UF_PROVA"]

#Criando um dataframe somente com as colunas selecionadas

df_notas_gold = df_enem_gold.select(select_columns)

df_notas_gold = df_notas_gold.withColumnRenamed("SG_UF_PROVA","UF_PROVA")

df_notas_gold = df_notas_gold.withColumnRenamed('NU_NOTA_CN','NOTA_CIENCIA_NATUREZA')

df_notas_gold = df_notas_gold.withColumnRenamed('NU_NOTA_CH','NOTA_CIENCIAS_HUMANAS')

df_notas_gold = df_notas_gold.withColumnRenamed('NU_NOTA_LC','NOTA_LINGUAGEM_CODIGOS')

df_notas_gold = df_notas_gold.withColumnRenamed('NU_NOTA_MT','NOTA_MATEMATICA')

df_notas_gold = df_notas_gold.withColumnRenamed('NU_NOTA_REDACAO','NOTA_REDACAO')

display(df_notas_gold)

# COMMAND ----------

# DBTITLE 1,Removendo os valores nulos dos atributos
#Selecionando as colunas
cols = ['INSCRICAO','NOTA_CIENCIA_NATUREZA','NOTA_CIENCIAS_HUMANAS','NOTA_LINGUAGEM_CODIGOS','NOTA_MATEMATICA','NOTA_REDACAO']

#Instancionando um dataframe sem o nulos
df_notas_semnulo_gold = df_notas_gold.na.drop(subset=cols)

#Exibindo o dataframe
display(df_notas_semnulo_gold)

# COMMAND ----------

#Modificando o tipo da coluna para inteiro,
#porque está dando ruim quando mudo uma string
#Ex: Quando mudo o numero 1 ele tbm faz alteração no numeros que contem 1x , x1...

from pyspark.sql.functions import col

df_enem_gold = df_enem_gold.withColumn('FAIXA_ETARIA',col('FAIXA_ETARIA').cast("integer"))

# COMMAND ----------

# DBTITLE 1,Especificando as Faixa Etaria
#Neste código, estamos usando o padrão ^1$, onde ^ representa o início da linha e $ representa o final da linha. Portanto, estamos garantindo que "1" seja #substituído apenas quando for uma correspondência exata na coluna "FAIXA_ETARIA". Isso resolverá o problema de substituição incorreta quando "1" está #contido em outros números, como "10".

from pyspark.sql.functions import regexp_replace

df_notas_semnulo_gold = df_notas_semnulo_gold.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^1$","Menor de 17 anos"))

df_notas_semnulo_gold = df_notas_semnulo_gold.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^2$","17 anos"))

df_notas_semnulo_gold = df_notas_semnulo_gold.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^3$","18 anos"))

df_notas_semnulo_gold = df_notas_semnulo_gold.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^4$","19 anos"))

df_notas_semnulo_gold = df_notas_semnulo_gold.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^5$","20 anos"))

df_notas_semnulo_gold = df_notas_semnulo_gold.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^6$","21 anos"))

df_notas_semnulo_gold = df_notas_semnulo_gold.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^7$","22 anos"))

df_notas_semnulo_gold = df_notas_semnulo_gold.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^8$","23 anos"))

df_notas_semnulo_gold = df_notas_semnulo_gold.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^9$","24 anos"))

df_notas_semnulo_gold = df_notas_semnulo_gold.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^10$","25 anos"))

df_notas_semnulo_gold = df_notas_semnulo_gold.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^11$","Entre 26 e 30 anos"))

df_notas_semnulo_gold = df_notas_semnulo_gold.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^12$","Entre 31 e 35 anos"))

df_notas_semnulo_gold = df_notas_semnulo_gold.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^13$","Entre 36 e 40 anos"))

df_notas_semnulo_gold = df_notas_semnulo_gold.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^14$","Entre 41 e 45 anos"))

df_notas_semnulo_gold = df_notas_semnulo_gold.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^15$","Entre 46 e 50 anos"))

df_notas_semnulo_gold = df_notas_semnulo_gold.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^16$","Entre 51 e 55 anos"))

df_notas_semnulo_gold = df_notas_semnulo_gold.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^17$","Entre 56 e 60 anos"))

df_notas_semnulo_gold = df_notas_semnulo_gold.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^18$","Entre 61 e 65 anos"))

df_notas_semnulo_gold = df_notas_semnulo_gold.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^19$","Entre 66 e 70 anos"))

df_notas_semnulo_gold = df_notas_semnulo_gold.withColumn("FAIXA_ETARIA", regexp_replace("FAIXA_ETARIA","^20$","Maior 70"))



# COMMAND ----------

display(df_notas_semnulo_gold)

# COMMAND ----------

# DBTITLE 1,Media das notas por estado
df_media_notas_estado = df_notas_semnulo_gold.groupBy("UF_PROVA").agg(
    round(avg(col("NOTA_CIENCIA_NATUREZA")),2).alias("MEDIA_CIENCIA_NATUREZA"),
    round(avg(col("NOTA_CIENCIAS_HUMANAS")),2).alias("MEDIA_CIENCIA_HUMANAS"),
    round(avg(col("NOTA_LINGUAGEM_CODIGOS")),2).alias("MEDIA_LINGUAGEM_CODIGOS"),
    round(avg(col("NOTA_MATEMATICA")),2).alias("MEDIA_MATEMATICA"),
    round(avg(col("NOTA_REDACAO")),2).alias("MEDIA_REDACAO")
)

display(df_media_notas_estado)

# COMMAND ----------

# DBTITLE 1,Salvando em arquivo delta na camada gold
df_media_notas_estado.write.format("delta")\
    .mode('overwrite')\
    .option("overwriteSchema", "true")\
    .save("/mnt/storageenem2000/gold/enem_notas_agg")
