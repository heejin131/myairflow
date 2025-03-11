from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
import pendulum

# Directed Acyclic Graph
with DAG(
    "seoul",
    #schedule=timedelta(days=1),
    #schedule="0 * * * *",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 3, 10, tz="Asia/Seoul"),
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    b1 = BashOperator(task_id="b_1",
                      bash_command="""
                      echo "date =================================> 'date'"
                      echo "data_interval_start ==================> {{data_interval_start}}"
                      echo "data_interval_end ====================> {{data_interval_end}}"
                      echo "logical_date =========================> {{logical_date}}"
                      echo "ds_nodash=============================> {{logical_date | ds_nodash}}"
                      echo "ts  ===================================> {{logical_date | ts}}"
                      echo "ts_nodash_with_tz=====================> {{ts_nodash_with_tz}}"
                      echo "ts_nodash=============================> {{logical_date | ts_nodash}}"
                      echo "prev_data_interval_start_success =====> {{prev_data_interval_start_success}}"
                      echo "prev_data_interval_end_success =====> {{prev_data_interval_end_success}}"
                      echo "prev_start_date_success =====> {{prev_start_date_success}}"
                      echo "prev_end_date_success =====> {{prev_end_date_success}}"
                      echo "inlet_events =====> {{inlet_events}}"
                      echo "outlets ===================> {{outlets}}"
                      echo "execution_date===================> {{execution_date}}"
                      echo "next_execution_date===================> {{next_execution_date}}"
                      echo "next_ds===================> {{next_ds}}"
                      echo "next_ds_nodash ===================> {{next_ds_nodash}}"
                      echo "prev_execution_date===================> {{prev_execution_date}}"
                      echo "prev_ds===================> {{prev_ds}}"
                      echo "prev_ds_nodash  ===================> {{prev_ds_nodash}}"
                      echo "yesterday_ds===================> {{yesterday_ds}}"
                      echo "yesterday_ds_nodash===================> {{yesterday_ds_nodash}}"
                      echo "tomorrow_ds===================> {{tomorrow_ds}}"
                      echo "tomorrow_ds_nodash===================> {{tomorrow_ds_nodash}}"
                      echo "prev_execution_date_success===================> {{prev_execution_date_success}}"
                      echo "conf===================> {{conf}}"
                      """)
    b2_1 = BashOperator(task_id="b_2_1", bash_command="echo 2_1")
    b2_2 = BashOperator(task_id="b_2_2", bash_command="echo 2_2")
    
    start >> b1 >> [b2_1, b2_2] >> end 