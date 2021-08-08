import boto3
import pandas as pandas
import urllib
import urllib3

#criar cliente para interagi com s3
#s3_client = boto3.client('s3')

#s3_client.download_file('datalake-igti-jeferson', '')


s3=boto3.resource('s3')
http=urllib3.PoolManager()

#urllib.request.urlopen('https://download.inep.gov.br/microdados/microdados_enem_2019.zip')   #Provide URL
s3.meta.client.upload_fileobj(http.request('GET', 'https://download.inep.gov.br/microdados/microdados_enem_2019.zip', preload_content=False), 'datalake-igti-jeferson', Key = "AKIAUGYPE5IQCUWBZPZX")