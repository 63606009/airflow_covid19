import json
from datetime import datetime
from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import requests

def get_ethics_commission_report():
    url = 'https://data.sfgov.org/resource/pv99-gzft.json'
    response = requests.get(url)
    dataSet = response.json()

    return dataSet


def save_data_into_db():
    dataSet = get_ethics_commission_report()
    for data in dataSet:
        import mysql.connector
        db = mysql.connector.connect(host='49.0.199.27',user='root',passwd='password',db='covid19')
        cursor = db.cursor()
        txn_date = data['txn_date'].replace('\n',' ')
        new_case = data['new_case']
        new_case_excludeabroad = data['new_case_excludeabroad'].replace('\n',' ')
        new_death = data['new_death'].replace('\n',' ')
        total_case =  data['total_case'].replace('\n',' ')
        total_death = data['total_death'].replace('\n',' ')
        total_recovered = data['total_recovered'].replace('\n',' ')
        update_date = data['update_date'].replace('\n',' ')
        cursor.execute('INSERT INTO ethics_commission_reports (txn_date,new_case, new_case_excludeabroad,new_death,total_death,total_recovered,update_date )'
                  'VALUES("%s", "%s", "%s","%s", "%s", "%s", "%s")',
                   (txn_date,new_case, new_case_excludeabroad,new_death,total_death, total_recovered, update_date))
        db.commit()
        print("Record inserted successfully into testcsv table")
        cursor.close()


default_args = {
    'owner': 'game',
    'start_date': datetime(2021, 11, 27),
    'email': ['63606009@kmitl.ac.th'],
}
with DAG('sf_ethics_commission',
         schedule_interval='@daily',
         default_args=default_args,
         description='ethics commission report',
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='ethics_commission_report',
        python_callable=get_ethics_commission_report
    )

    t2 = PythonOperator(
        task_id='save_data_into_db',
        python_callable=save_data_into_db
    )

    t1 >> t2 