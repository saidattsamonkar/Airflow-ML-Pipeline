# Airflow PySpark ML Pipeline 

### Introduction
This Airflow project is created using Astro CLI. The aim is to develop a ML pipeline for detecting presence or stages of Hepatitis-C Virus (https://archive.ics.uci.edu/ml/datasets/HCV+data). 

### Project DAG
![DAG](https://github.com/saidattsamonkar/Airflow-ML-Pipeline/blob/main/assets/dag.png)

### Steps in the Pipeline
- Clean and transform data from an Amazon S3 bucket
- Train 4 Machine Learning Models in parallel (Logistic Regression, Decision Tree, Naive Bayes, Random Forest)
- Evaluate all models on Key metrics (accuracy, precision, recall, F1 score)
- Model Selection and update model on Amazon S3

### Grant Chart
The run times of each task is displayed in the chart
![GRANT CHART](https://github.com/saidattsamonkar/Airflow-ML-Pipeline/blob/main/assets/grant_chart.png)

### Steps to run this project locally

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
