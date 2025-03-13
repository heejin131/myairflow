from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonVirtualenvOperator, PythonOperator 
import pendulum

with DAG(
    "myetl",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 3, 13, tz="Asia/Seoul"),
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    def load_data_py(timestamp):
        import pandas as pd
        df = pd.read_csv(f"/home/gmlwls5168/data/{timestamp}/data.csv")
        df.to_parquet(f"/home/gmlwls5168/data/{timestamp}/data.parquet", engine='pyarrow')
        df_loaded = pd.read_parquet('data.parquet', engine='pyarrow')
        print(df_loaded)
    
    def agg_data_py(timestamp):
        import pandas as pd
        import pendulum
        df = pd.read_parquet(f"/home/gmlwls5168/data/{timestamp}/data.parquet")
        agg_df = df.groupby(['name', 'value']).size().reset_index(name='count')
        agg_df.to_csv(f'/home/gmlwls5168/data/{timestamp}/agg.csv', index=False)
        agg_df['timestamp'] = pendulum.now('Asia/Seoul').format('YYYY-MM-DDTHH:mm:ss.SSSSSS[Z]')
        
    make_data=BashOperator(task_id="make", bash_command="bash /home/gmlwls5168/airflow/make_data.sh /home/gmlwls5168/data/{{data_interval_start.in_tz('Asia/Seoul').format('YYYY/MM/DD/HH')}}")
    load_data=PythonVirtualenvOperator(task_id="load", 
                                       python_callable=load_data_py,
                                       requirements=["pandas","pyarrow"],
                                       op_args=["{{ data_interval_start.in_tz('Asia/Seoul').format('YYYY/MM/DD/HH')}}"])
    agg_data=PythonVirtualenvOperator(task_id="agg",
                                      python_callable=agg_data_py,
                                      requirements=["pandas","pyarrow","pendulum"],
                                      op_args=["{{ data_interval_start.in_tz('Asia/Seoul').format('YYYY/MM/DD/HH')}}"]
                                      )

start >> make_data >> load_data >> agg_data >> end

if __name__ == "__main__":
    dag.test()