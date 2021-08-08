from pyspark.sql.functions import mean, max, min, col,count
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("ExerciseSpark")
    .getOrCreate()
)

#Ler dados do enem 2019
enem = (
    spark
    .read
    .format("csv")
    .option("header", True)
    .option("inferSchema",True)
    .option("delimiter",";")
    .option("encoding","utf-8")
    .load('s3://datalake-igti-jeferson/raw-data/ENEM/year-2019/MICRODADOS_ENEM_2019.csv')
)

#Salvar parquet

(
    enem
    .write
    .mode("overwrite")
    .format("parquet")
    .save("s3://datalake-igti-jeferson/sataging/enem")
)
