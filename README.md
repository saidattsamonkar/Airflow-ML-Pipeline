# Airflow PySpark ML Pipeline 

## Introduction
This Airflow project is created using Astro CLI. The aim is to develop a ML pipeline for detecting presence or stages of Hepatitis-C Virus (https://archive.ics.uci.edu/ml/datasets/HCV+data). <br /><br />

## Project DAG
![DAG](https://github.com/saidattsamonkar/Airflow-ML-Pipeline/blob/main/assets/dag.png) <br /><br />

## Steps in the Pipeline
**1. Clean and transform** <br />
 Fetch data from an Amazon S3 bucket using boto 3. Clean and transform the data (handling null values, cleaning string values, encoding categorical values etc) and split the data into train and test sets. Upload the ```train.csv``` and ```test.csv``` datasets to the Amazon S3 bucket
 
**2. Train Machine Learning Models** <br /> 
 We run 4 tasks in parallel that loads the ```train.csv``` file and trains 4 ML models using PySpark ML. The steps also load the ```test.csv``` files and make predictions. These predictions are uploaded in the S3 bucket. The 4 ML models used are:
 - Logistic Regression
 - Decision Tree Classifier
 - Naive Bayes Classifier
 - Random Forest Classifier
 
**3. Evaluate Models** <br /> 
 This step load the predictions of all 4 models and generate key metrics (accuracy, precision, recall, F1 score) to create ```evaluation_summary.csv``` which is then uploaded to the S3 bucket.
 
**4. Model Selection** <br /> 
 This step reads the ```evaluation_summary.csv``` file from the S3 bucket and selects a model depending on the model selection strategy. This model is then made available as a compressed file on the S3 bucket called ```final_model.tar.gz``` <br /><br />

### Grant Chart
The run times of each task is displayed in the chart
![GRANT CHART](https://github.com/saidattsamonkar/Airflow-ML-Pipeline/blob/main/assets/grant_chart.png) <br /><br />

## Steps to run this project locally

1. **Upload Data to an S3 bucket** <br />
Create an S3 bucket and upload the data file from https://archive.ics.uci.edu/ml/datasets/HCV+data. Creae an IAM User with ```AmazonS3FullAccess``` previledge in AWS and download the access keys for the User.



 Start Airflow on your local machine by running 'astro dev start'.

This command will spin up 3 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks

 Verify that all 3 Docker containers were created by running 'docker ps'.

Note: Running 'astro dev start' will start your project with the Airflow Webserver exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either stop your existing Docker containers or change the port.

 Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password.

You should also be able to access your Postgres Database at 'localhost:5432/postgres'.

Deploy Your Project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://docs.astronomer.io/cloud/deploy-code/

Contact
=======

The Astronomer CLI is maintained with love by the Astronomer team. To report a bug or suggest a change, reach out to our support team: https://support.astronomer.io/
