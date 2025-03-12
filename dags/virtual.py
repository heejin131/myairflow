from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonVirtualenvOperator
import pendulum

with DAG(
    "virtual",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 3, 12, tz="Asia/Seoul"),
    default_args={
        "depends_on_past" : False,
    },
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    def f_vpython():
        from myairflow.send_notify import send_noti
        send_noti("python virtualenv operator : HEEJIN : vpython")    
    
    def f_python():
        from myairflow.send_notify import send_noti
        send_noti("python virtualenv operator : HEEJIN python")
            
    t_vpython = PythonVirtualenvOperator(
            task_id = "t_vpython",
            python_callable=f_vpython,
            requirements=[
                "git+https://github.com/heejin131/myairflow.git@0.1.0"
            ]
        )

    t_python = PythonVirtualenvOperator(task_id="t_python", python_callable=f_python)

    start >> t_vpython >> t_python >> end

if __name__ == "__main__":
    dag.test()