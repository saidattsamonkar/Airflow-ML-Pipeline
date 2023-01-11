from datetime import datetime, date
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
from CleanAndTransform import func_CNT
from LogReg import func_LR
from RandForClf import func_RFC
from NaiBay import func_NB
from DecTreClf import func_DTC
from ModelEval import func_Evaluate
from ModelSelec import func_MS
import os

os.system('export JAVA_HOME= "$(dirname $(dirname $(readlink -f $(which java))))"')

#Setting AWS User secret access keys
os.environ["AWS_ACCESS_KEY_ID"] = "AKIA423CVPKMS3WNYR7D"
os.environ["AWS_SECRET_ACCESS_KEY"] = "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"

default_args= {
    'owner': 'Saidatt S Amonkar',
    'email_on_failure': False,
    'email': ['sinaiamonkar.s@outlook.com'],
    'start_date': datetime(2022,1,1)
}

with DAG("ml_pipeline",description='End-to-end ML pipeline for Blood Donor Classification',
    schedule_interval='@daily',start_date=datetime(2021,1,1), catchup=False) as dag:

    cleanTransform = PythonOperator(
        task_id='Cleaning_And_Transform',
        python_callable = func_CNT
    )


    TrainLR = PythonOperator(
        task_id='Logistic_Regression_Train',
        python_callable = func_LR
    )

    TrainRFC = PythonOperator(
        task_id='Random_Forest_Classifier_Train',
        python_callable = func_RFC
    )

    TrainNB = PythonOperator(
        task_id='Naive_Bayes_Train',
        python_callable = func_NB
    )

    TrainDTC = PythonOperator(
        task_id='Decision_Tree_Classifier_Train',
        python_callable = func_DTC
    )

    ModelEvaluate = PythonOperator(
        task_id='Model_Evaluation',
        python_callable = func_Evaluate
    )

    ModelSelect = PythonOperator(
        task_id='Model_Selection',
        python_callable = func_MS
    )

cleanTransform >> TrainLR 
cleanTransform >> TrainRFC
cleanTransform >> TrainNB
cleanTransform >> TrainDTC

TrainLR >> ModelEvaluate
TrainRFC >> ModelEvaluate
TrainNB >> ModelEvaluate
TrainDTC >> ModelEvaluate

ModelEvaluate >> ModelSelect