import boto3
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pandas as pd
import csv
import io
import pyspark.ml
from pyspark.ml.feature import StringIndexer
from io import BytesIO
import os

def func_CNT():

    sc = SparkContext(master='local[2]')
    spark = SparkSession.builder.appName("ML").getOrCreate()

    s3 = boto3.client('s3',
    aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name='us-east-1'
    ) 

    obj = s3.get_object(Bucket='airflow-customer-churn-mlops', Key='hcvdata.csv') 
    data = obj['Body'].read().decode('utf-8') 

    data_frame = pd.read_csv(io.StringIO(data))

    # Convert the pandas DataFrame into a Spark DataFrame
    df = spark.createDataFrame(data_frame)

    df = df.select('Age', 'Sex', 'ALB', 'ALP', 'ALT', 'AST', 'BIL', 'CHE', 'CHOL', 'CREA', 'GGT', 'PROT', 'Category')

    genderEncoder = StringIndexer(inputCol='Sex', outputCol='Gender').fit(df)
    df = genderEncoder.transform(df)

    catEncoder = StringIndexer(inputCol='Category', outputCol='Target').fit(df)
    df = catEncoder.transform(df)

    df = df.select('Age', 'Gender', 'ALB', 'ALP', 'ALT', 'AST', 'BIL', 'CHE', 'CHOL', 'CREA', 'GGT', 'PROT', 'Target')

    df = df.toPandas()
    for col in ['Age', 'ALB', 'ALP', 'ALT', 'AST', 'BIL', 'CHE', 'CHOL', 'CREA', 'GGT', 'PROT']:    
        df[col] = df[col].replace('NA',df[col].mean()).astype(float)
    df=df.fillna(df.mean(axis=0))

    df = df.astype(float)
    df=spark.createDataFrame(df)

    train_df, test_df = df.randomSplit([0.7,0.3])

    train_df = train_df.toPandas()
    buffer = BytesIO()
    train_df.to_csv(buffer)
    s3.put_object(Bucket='airflow-customer-churn-mlops', Key='train_data.csv', Body=buffer.getvalue())

    test_df = test_df.toPandas()
    buffer = BytesIO()
    test_df.to_csv(buffer)
    s3.put_object(Bucket='airflow-customer-churn-mlops', Key='test_data.csv', Body=buffer.getvalue())

    buffer.close()



