

import boto3
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pandas as pd
import csv
import io
import pyspark.ml
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import  RandomForestClassifier
import pyspark.sql.functions as F
from io import BytesIO
import os
import shutil
import pickle

def func_RFC(**kwargs):

    sc = SparkContext(master='local[2]')
    spark = SparkSession.builder.appName("ML").getOrCreate()

    s3 = boto3.client('s3',
    aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name='us-east-1'
    ) 

    obj = s3.get_object(Bucket='airflow-customer-churn-mlops', Key='train_data.csv') 
    data = obj['Body'].read().decode('utf-8') 

    data_frame = pd.read_csv(io.StringIO(data))

    # Convert the pandas DataFrame into a Spark DataFrame
    df = spark.createDataFrame(data_frame)

    df = df.select("*").orderBy(F.rand())
    req_features = ['Age', 'Gender', 'ALB', 'ALP', 'ALT', 'AST', 'BIL', 'CHE', 'CHOL', 'CREA', 'GGT', 'PROT']
    va = VectorAssembler(inputCols=req_features,outputCol = 'features')
    train_df = va.transform(df)

    rfc = RandomForestClassifier(featuresCol='features',labelCol='Target')
    rfc_model = rfc.fit(train_df)

    
    obj = s3.get_object(Bucket='airflow-customer-churn-mlops', Key='test_data.csv') 
    data = obj['Body'].read().decode('utf-8') 

    data_frame = pd.read_csv(io.StringIO(data))

    # Convert the pandas DataFrame into a Spark DataFrame
    df = spark.createDataFrame(data_frame)

    df = df.select("*").orderBy(F.rand())
    req_features = ['Age', 'Gender', 'ALB', 'ALP', 'ALT', 'AST', 'BIL', 'CHE', 'CHOL', 'CREA', 'GGT', 'PROT']
    va = VectorAssembler(inputCols=req_features,outputCol = 'features')
    test_df = va.transform(df)

    y_pred = rfc_model.transform(test_df)

    y_pred = y_pred.toPandas()
    y_pred.to_csv('rfc_y_pred.csv')
    with open('rfc_y_pred.csv', 'rb') as f:
        s3.put_object(Bucket='airflow-customer-churn-mlops', Key='rfc_y_pred.csv', Body=f)

        
    #kwargs['ti'].xcom_push(key='rfc_y_pred', value='rfc_y_pred.csv')

    rfc_model.write().overwrite().save('rfc_model.model')
    shutil.make_archive('rfc_model', 'gztar', root_dir='rfc_model.model')
    with open('rfc_model.tar.gz', 'rb') as f:
        s3.put_object(Bucket='airflow-customer-churn-mlops', Key='rfc_model.tar.gz', Body=f)
    


