import boto3
import gzip
from io import BytesIO

s3 = boto3.client('s3')
s3.upload_fileobj(
    Fileobj=gzip.GzipFile(
        None,
        'rb',
        fileobj=BytesIO(
            s3.get_object(Bucket='datalake-igti-jeferson', Key="AKIAUGYPE5IQCUWBZPZX")['Body'].read())),
    Bucket='datalake-igti-jeferson/raw-data/ENEM/2019',
    Key="AKIAUGYPE5IQCUWBZPZX")