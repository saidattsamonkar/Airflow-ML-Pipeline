import boto3
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pandas as pd
import csv
import io
import os
from pyspark.mllib.evaluation import MulticlassMetrics


def func_Evaluate():

    sc = SparkContext(master='local[2]')
    spark = SparkSession.builder.appName("ML").getOrCreate()

    s3 = boto3.client('s3',
    aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name='us-east-1'
    ) 

    eval_sum = []

    for model in ['lr','nb','dtc','rfc']:

        obj = s3.get_object(Bucket='airflow-customer-churn-mlops', Key= model+'_y_pred.csv') 
        data = obj['Body'].read().decode('utf-8') 

        y_pred = pd.read_csv(io.StringIO(data))

        # Convert the pandas DataFrame into a Spark DataFrame
        y_pred = spark.createDataFrame(y_pred)

        metric = MulticlassMetrics(y_pred['Target','prediction'].rdd)

        eval_sum +=  [[model,metric.accuracy,metric.precision(1.0),metric.recall(1.0),metric.fMeasure(1.0)]]

    eval_sum_df = pd.DataFrame(eval_sum,columns=['model','accuracy','precision','recall','F1_score'])
    eval_sum_df = eval_sum_df.sort_values(by='F1_score', ascending=False)

    eval_sum_df.to_csv('evaluation_summary.csv')
    with open('evaluation_summary.csv', 'rb') as f:
        s3.put_object(Bucket='airflow-customer-churn-mlops', Key='evaluation_summary.csv', Body=f)




