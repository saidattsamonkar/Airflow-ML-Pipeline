import boto3
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pandas as pd
import csv
import io
import os

def func_MS():

    s3 = boto3.client('s3',
    aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name='us-east-1') 

    obj = s3.get_object(Bucket='airflow-customer-churn-mlops', Key= 'evaluation_summary.csv') 
    data = obj['Body'].read().decode('utf-8') 

    eval_sum_df = pd.read_csv(io.StringIO(data))

    eval_sum_df = eval_sum_df.sort_values(by='F1_score', ascending=False)

    model = eval_sum_df['model']
    model = model[0]

    s3.copy_object(Bucket='airflow-customer-churn-mlops',
    CopySource={'Bucket': 'airflow-customer-churn-mlops',
    'Key': model+'_model.tar.gz'},
    Key='final_model.tar.gz')

    
