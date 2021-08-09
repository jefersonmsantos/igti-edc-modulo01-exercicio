from pyspark.sql import SparkSession 
from pyspark.sql import col, min, max 

#Cria objeto da Spark Session
spark = (SparkSession.builder.appName("DeltaExercise")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .geetOrCreate()
)

#Importa o modulo das tabelas delta
from detla.tables import *

#leitura de dados

enem = (
    spark.read.format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .option("delimiter", ";")
    .load('s3://datalake-igti-jeferson/raw-data/ENEM/')
)

#Escreve a tabela em staging em formato delta
print("Writing delta table...")
(
    enem
    .write
    .mode("overwrite")
    .format("delta")
    .save('s3://datalake-igti-tf-producao-289405200928/staging-zone/ENEM')
)