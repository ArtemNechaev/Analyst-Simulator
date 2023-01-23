import pandas as pd
from datetime import timedelta, datetime
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

default_conn = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20221120'
}

test_conn = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': '656e2b0c9c',
    'user': 'student-rw',
    'database': 'test'
}

default_args = {
    'owner': 'a-nechaev',
    'retries': 2,
    'retry_delay': timedelta(minutes = 3),
    'start_date': days_ago(1),
    'depends_on_past': False,
    'date': '{{ds}}'
}

def get_slice(df, slice_column, 
                  res_columns = ['views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent'] ):
        
    result = df[[slice_column] + res_columns]\
        .groupby(slice_column, as_index=False)\
        .sum()\
        .rename(columns={slice_column:'dimension_value'})\
        .astype({k: int for k in res_columns})

    result['dimension'] = slice_column
        
    return result
    

@dag(default_args = default_args, schedule_interval='0 3 * * *',catchup=False)
def l6_anechaev(): 

    @task()
    def extract_messages(date):
        q=f"""
            SELECT *
            FROM(
              SELECT 
                user_id u_id,
                gender, age, os,
                count(reciever_id ) messages_sent,
                count(DISTINCT reciever_id ) users_sent
              FROM simulator_20221120.message_actions 
              WHERE toDate(time) = toDate('{date}')
              GROUP BY u_id, gender, age, os

            ) t
            full outer join
            (
              SELECT 
                reciever_id  as u_id,
                gender, age, os,
                count(user_id) messages_received,
                count(DISTINCT user_id) users_received
              FROM simulator_20221120.message_actions 
              WHERE toDate(time) = toDate('{date}')
              GROUP BY u_id, gender, age, os
            ) b
            using u_id, gender, age, os
        """
        return ph.read_clickhouse(q, connection=default_conn)
    
    @task()
    def extract_feed(date):
        q = f"""
            SELECT 
              user_id u_id,
              gender, age, os,
              SUM(action ='like') likes,
              SUM(action ='view') views
            FROM simulator_20221120.feed_actions
            WHERE toDate(time) = toDate('{date}')
            GROUP BY u_id, gender, age, os

            """
        return ph.read_clickhouse(q, connection=default_conn)
    
    @task()
    def merge(feed_df, message_df):
        merged = feed_df.merge(message_df, how='outer').fillna(0)
        return merged

    @task()
    def age_slice(merged):
        return get_slice(merged, 'age')
        
    @task()
    def gender_slice(merged):
        return get_slice(merged, 'gender')
    
    @task()
    def os_slice(merged):
        return get_slice(merged, 'os')
    
    
    @task()
    def load(date, dfs = []):
        
        q = f"""
            CREATE TABLE IF NOT EXISTS {test_conn['database']}.l6_anechaev 
            (
                event_date Date,
                dimension String,
                dimension_value String,
                views UInt64,
                likes UInt64,
                messages_received UInt64,
                messages_sent UInt64,
                users_received UInt64,
                users_sent UInt64

            ) ENGINE = MergeTree()
            PRIMARY KEY (event_date, dimension, dimension_value)
        """
        del_q = f"""
            ALTER TABLE 
            {test_conn['database']}.l6_anechaev 
            DELETE
            WHERE event_date = toDate('{date}')
        """
        if dfs:
            # crate table if not exists
            ph.execute(query=q, connection=test_conn)

            #del rows where event_date = dag execution date 
            #this is necessary to restart tasks
            ph.execute(query=del_q, connection=test_conn)

            #concat slicies and add event date column
            df = pd.concat(dfs)
            df['event_date'] = datetime.strptime(date, '%Y-%m-%d')

            #insert new rows 
            ph.to_clickhouse(df, 'l6_anechaev', index=False, connection=test_conn)
    
    # task execution
    m = extract_messages()
    f = extract_feed()
    
    merged = merge(f,m)
    
    gender = gender_slice(merged)
    age = age_slice(merged)
    os = os_slice(merged)
    
    load(dfs=[gender, age, os])
    
#run DAG   
l6_anechaev = l6_anechaev()

    
        
        
