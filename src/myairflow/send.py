import pandas as pd
import time
import pendulum

def load_data_py(timestamp):
    df = pd.read_csv(f"/home/gmlwls5168/data/{timestamp}/data.csv")
    df.to_parquet(f"/home/gmlwls5168/data/{timestamp}/data.parquet", engine='pyarrow')
    df_loaded = pd.read_parquet('data.parquet', engine='pyarrow')
    print(df_loaded)

def agg_data_py(timestamp):
    df = pd.read_parquet(f"/home/gmlwls5168/data/{timestamp}/data.parquet")
    agg_df = df.groupby(['name', 'value']).size().reset_index(name='count')
    agg_df.to_csv(f'/home/gmlwls5168/data/{timestamp}/agg.csv', index=False)
    agg_df['timestamp'] = pendulum.now('Asia/Seoul').format('YYYY-MM-DDTHH:mm:ss.SSSSSS[Z]')