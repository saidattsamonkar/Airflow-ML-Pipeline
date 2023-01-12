# Airflow PySpark ML Pipeline 

## Introduction <br />
This Airflow project is created using Astro CLI. The aim is to develop a ML pipeline for detecting presence and/or detecting stages of Hepatitis-C Virus (https://archive.ics.uci.edu/ml/datasets/HCV+data). <br /><br /><br />

## Project DAG <br />
![DAG](https://github.com/saidattsamonkar/Airflow-ML-Pipeline/blob/main/assets/dag.png) <br /><br /><br />

## Steps in the Pipeline <br />
**1. Clean and transform** <br />
 This task fetches data from an Amazon S3 bucket using boto 3. Clean and transform the data (handling null values, cleaning string values, encoding categorical values etc) and split the data into train and test sets. Upload the ```train.csv``` and ```test.csv``` datasets to the Amazon S3 bucket
 <br /><br />
**2. Train Machine Learning Models** <br /> 
 We run 4 tasks in parallel that loads the ```train.csv``` file and trains 4 ML models using PySpark ML. The steps also load the ```test.csv``` files and make predictions. These predictions are uploaded in the S3 bucket. The 4 ML models used are:
 - Logistic Regression
 - Decision Tree Classifier
 - Naive Bayes Classifier
 - Random Forest Classifier
 <br /><br />
 
**3. Evaluate Models** <br /> 
 This task load the predictions of all 4 models and generate key metrics (accuracy, precision, recall, F1 score) to create ```evaluation_summary.csv``` which is then uploaded to the S3 bucket.<br /><br />
 
 <img src="https://github.com/saidattsamonkar/Airflow-ML-Pipeline/blob/main/assets/evaluation_summary.png" width="450" height="125" />
 
<br />
 
**4. Model Selection** <br /> 
 This task reads the ```evaluation_summary.csv``` file from the S3 bucket and selects a model depending on the model selection strategy. This model is then made available as a compressed file on the S3 bucket called ```final_model.tar.gz``` <br /><br />
<br /><br />
## Grant Chart <br />
The run times of each task is displayed in the chart <br /><br />
![GRANT CHART](https://github.com/saidattsamonkar/Airflow-ML-Pipeline/blob/main/assets/grant_chart.png) <br /><br />
<br /><br />
## Steps to run this project locally <br />

**1. Upload Data to an S3 bucket** <br />
Create an S3 bucket and upload the data file from https://archive.ics.uci.edu/ml/datasets/HCV+data. Creae an IAM User with ```AmazonS3FullAccess``` previledge in AWS and download the access keys for the User.
<br /><br />
**2. Download Astro CLI** <br />
Follow the instructions on the astronomer web page - https://docs.astronomer.io/astro/cli/install-cli
<br /><br />
**3. Download Docker** <br />
Download (https://docs.docker.com/desktop/install/mac-install/) and start Docker Desktop
<br /><br />
**4. Clone Repsitory** <br />
Clone this repository using the command ```$ git clone https://github.com/saidattsamonkar/Airflow-ML-Pipeline```. Open the project with VSCode (preferred).
Add your AWS access keys by updating ```os.environ["AWS_ACCESS_KEY_ID"]``` and ```os.environ["AWS_SECRET_ACCESS_KEY"]``` in ```\Dags\main.py```
<br /><br />
**5. Start Airflow** <br />
Run command ```astro dev start``` inside the VSCode terminal or after cd into the project directory. This command will spin up 3 Docker containers on your machine, each for a different Airflow component.
<br /><br />
**5. Start Airflow** <br />
Run command ```astro dev start``` inside the VSCode terminal or after cd into the project directory. This command will spin up 3 Docker containers on your machine, each for a different Airflow component. Verify that all 3 Docker containers were created by running the command ```docker ps```. Acess the Airflow UI at http://localhost:8080/ and find the ```ml-pipeline``` under Dags. Finally toggle the dag on and trigger the dag.

