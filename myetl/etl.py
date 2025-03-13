from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonVirtualenvOperator, PythonOperator
import pendulum

with DAG(
    "myetl2",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 3, 13, tz="Asia/Seoul"),
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    def load():
    from myetl.myetl_db import load_data_py
    load_data_py(f"load")

    def agg():
    from myetl.myetl_db import agg_data_py
    agg_data_py(f"agg")

    make_data=BashOperator(task_id="make",
                           bash_command="bash /home/gmlwls5168/airflow/make_data.sh /home/gmlwls5168/data/{{data_interval_start.in_tz('Asia/Seoul').format('YYYY/MM/DD/HH')}}")
    load_data=PythonVirtualenvOperator(task_id="load",
                                       python_callable=load,
                                       requirements=["git+https://github.com/heejin131/myairflow.git@0.1.2"],
                                       op_args=["{{ data_interval_start.in_tz('Asia/Seoul').format('YYYY/MM/DD/HH')}}"])
    agg_data=PythonVirtualenvOperator(task_id="agg",
                                      python_callable=agg,
                                      requirements=["git+https://github.com/heejin131/myairflow.git@0.1.2"],
                                      op_args=["{{ data_interval_start.in_tz('Asia/Seoul').format('YYYY/MM/DD/HH')}}"]
                                      )

start >> make_data >> load_data >> agg_data >> end

if __name__ == "__main__":
    dag.test()

