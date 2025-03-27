from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonVirtualenvOperator
from airflow.sensors.filesystem import FileSensor

DAG_ID = "movie_after"

with DAG(
    DAG_ID,
    default_args={
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=5,
    description='movie',
    schedule="10 10 * * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 5),
    catchup=True,
    tags=['api', 'movie'],
) as dag:
    REQUIREMENTS = ["git+https://github.com/heejin131/movie.git@0.4.0"]
    BASE_DIR = f"/home/gmlwls5168/data/{DAG_ID}"

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id = 'end')

    #경로에 파일이 존재하는 지 확인하는 작업
    check_done = FileSensor(
        task_id="check.done",
        filepath="/home/gmlwls5168/data/movies/done/dailyboxoffice/{{ds_nodash}}/_DONE",
        fs_conn_id="fs_after_movie",
        poke_interval=180,  # 3분마다 체크
        timeout=3600,  # 1시간 후 타임아웃
        mode="reschedule",  # 리소스를 점유하지 않고 절약하는 방식
    )

    #경로에 파일을 저장하는 작업
    def fn_gen_meta(ds_nodash, **kwargs):
        import json
        import pandas as pd
        from movie.api.call import fillna_meta
        
        print(json.dumps(kwargs, indent=4, ensure_ascii=False))
        
        try:
            previous_df = pd.read_parquet('/home/gmlwls5168/data/movie_after/meta/meta.parquet')
        except FileNotFoundError:
            previous_df = pd.DataFrame()      
        current_df = pd.read_parquet('/home/gmlwls5168/data/movies/dailyboxoffice/dt={ds_nodash}')
        
        meta_df = fillna_meta(previous_df, current_df)
        
        save_path = "/home/gmlwls5168/data/movie_after/meta/meta.parquet"
        meta_df.to_parquet(save_path)
        print(meta_df)

    gen_meta= PythonVirtualenvOperator(
        task_id='gen.meta',
        python_callable=fn_gen_meta,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={"ds_nodash": "{{ ds_nodash }}"}
    )

    def fn_gen_movie(base_path, ds_nodash, **kwargs):
        import json
        from movie.api.call import fillna_meta
        print(json.dumps(kwargs, indent=4, ensure_ascii=False))
        print(f"base_path: {base_path}")
        
        try:
            previous_df = pd.read_parquet(f"{base_path}/meta/meta.parquet")
        except FileNotFoundError:
            previous_df = pd.DataFrame()
        
        # TODO -> f"{base_path}/dailyboxoffice/ 생성
      
        current_df = pd.read_parquet('/home/gmlwls5168/data/movies/dailyboxoffice/dt={ds_nodash}')
        meta_df = fillna_meta(previous_df, current_df)
        
        meta_save_path = f"{base_path}/dailyboxoffice/"
        meta_df.to_parquet(meta_save_path, partition_cols=['dt', 'multiMovieYn', 'repNationCd'])
        print("merge fin")


    gen_movie = PythonVirtualenvOperator(
        task_id="gen.movie",
        python_callable=fn_gen_movie,
        requirements=REQUIREMENTS,
        system_site_packages=False,
        op_kwargs={"base_path": BASE_DIR},
    )

    make_done = BashOperator(
        task_id='make.done',
        bash_command="""
        DONE_BASE = $BASE_DIR/done
        echo $DONE_BASE
        mkdir -p $DONE_BASE/{{ ds_nodash }}
        touch $DONE_BASE/{{ ds_nodash }}/_DONE
        """,
        env={"BASE_DIR":BASE_DIR},
        append_env = True
    )

    start >> check_done >> gen_meta >> gen_movie >> make_done >> end