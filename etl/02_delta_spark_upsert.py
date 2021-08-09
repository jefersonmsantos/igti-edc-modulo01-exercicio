import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, lit 

#Configuracao de logs da aplicação
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('datalake_enem_small_upsert')
logger.setLevel(logging.DEBUG)

#Definição da Spark Session
spark = (SparkSession.builder.appName("DeltaExercise")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

logger.info("Importing delta.tables...")
from delta.tables import *

logger.info("Produzindo novos dados...")
enemnovo = (
    spark.read.format("delta")
    .load('s3://datalake-igti-tf-producao-289405200928/staging-zone/ENEM')
)

#Define algumas incricoes (chaves) que serao alteradas
inscricoes = [190001595656,
            190001421546,
            190001133210,
            190001199383,
            190001237802,
            190001782198,
            190001421548]

logger.info("Reduz a x casos e faz updates internos no municipio de residencia")
enemnovo = enemnovo.where(enemnovo.NU_INSCRICAO.isin(inscricoes))
enemnovo = enemnovo.withColumn("NO_MUNICIPIO_RESIDENCIA", lit("NOVA CIDADE")).withColumn("CO_MUNICIPIO_RESIDENCIA", lit(10000000))

logger.info("Pega os dados do Enem velhos na tabela Delta...")
enemvelho = DeltaTable.forPath(spark, 's3://datalake-igti-tf-producao-289405200928/staging-zone/ENEM')

logger.info("Realiza o UPSERT...")
(
    enemvelho.alias("old")
    .merge(enemnovo.alias("new"), "old.NU_INSCRICAO = new.NU_INSCRICAO")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

logger.info("Atualizacao completa! \n\n")

logger.info("Gera manifesto symlink...")
enemvelho.generate("symlink_format_manifest")

logger.info("Manifesto gerado.")
