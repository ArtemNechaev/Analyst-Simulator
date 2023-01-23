import pandas as pd
from datetime import timedelta, datetime
import pandahouse as ph
import io
import telegram
from telegram import InputMediaPhoto, InputMediaDocument
import matplotlib.pyplot as plt
import seaborn as sns
plt.rcParams.update({'font.size': 22})


from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.utils.dates import days_ago

BOT_TOKEN = '5892789921:AAGHn5cInwBv7V0QkfSZgN2fotTYzId5QSA'
chat_id = -744381656

bot = telegram.Bot(BOT_TOKEN)

default_conn = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20221120'
}

default_args = {
    'owner': 'a-nechaev',
    'retries': 2,
    'retry_delay': timedelta(minutes = 3),
    'start_date': days_ago(1),
    'depends_on_past': False
}


def extract_feed(groups):
    
    if isinstance(groups, list):
        groups = ','.join(groups) 
        
    q = f"""
        select 
            toStartOfFifteenMinutes(time) ts,
            toStartOfFifteenMinutes(date_add(minute, -15, now())) m,
            {groups},
            count(distinct user_id) feed_au,  
            sum(action='view') views,
            sum(action='like') likes,
            likes/views ctr
        from {default_conn['database']}.feed_actions
        where toDate(ts) between today() - 30 and today()
            and toMinute(ts) = toMinute(m)
            and toHour(ts) = toHour(m)
            --and toDayOfWeek(ts) = toDayOfWeek(m)
        group by ts, m, {groups}
        order by ts
    """
    df = ph.read_clickhouse(q, connection=default_conn).drop(['m'], axis=1)
    return df

def extract_message(groups):
    
    if isinstance(groups, list):
        groups = ','.join(groups) 

    q = f"""
        select 
            toStartOfFifteenMinutes(time) ts,
            toStartOfFifteenMinutes(date_add(minute, -15, now())) m,
            {groups},
            count(distinct user_id) message_au,
            count(user_id) num_messages
        from {default_conn['database']}.message_actions
        where toDate(ts) between today() - 30 and today()
            and toMinute(ts) = toMinute(m)
            and toHour(ts) = toHour(m)
            --and toDayOfWeek(ts) = toDayOfWeek(m)
        group by ts, m, {groups}
        order by ts
    """
    df = ph.read_clickhouse(q, connection=default_conn).drop(['m'], axis=1)
    return df


def save_fig(fig, name):
    plot_object = io.BytesIO()
    plot_object.name = name
    fig.savefig(plot_object)
    plot_object.seek(0)
    
    return plot_object

def iqr_alert(groups, extract_func, x='ts', a = 1.5 ):
    text = """
    Метрика *{0}* в срезе *{1}*\.
    Текущее значение *{2}*\.
    Отклонение более *{3}%*\.
    
    На графике представлены значения текущего 15\-ти минутного интервала за прошлые дни
    """
    data = extract_func(groups)
    
    for i, df in data.groupby(groups):
        df = df.set_index(x).drop(groups, axis=1)
        last = df.last('1D').iloc[0]

        q25 = df.quantile(0.25)
        q75 = df.quantile(0.75)
        IQR = q75-q25
        is_anomaly = (last < q25 - a*IQR) + (last > q75 + a*IQR)
        anomaly = (last/df.quantile(0.5)-1)*is_anomaly
        
        for metric_name, deviation in anomaly.to_dict().items():
            if deviation !=0:
                
                current_val = str(round(last[metric_name],2)).replace('.',',')
                slice_name =f'{groups}: {i}'
                
                message = text.format(metric_name.replace("_", " "), 
                                      slice_name, 
                                      current_val, 
                                      str(round(deviation * 100,0)).replace('.',','))
                
                ax = df.plot.line(y=metric_name, figsize=(12,8), grid=True)
                ax.set_title(f'{metric_name} {slice_name}')

                bot.sendPhoto(
                    chat_id=chat_id, 
                    photo=save_fig(ax.figure, metric_name), 
                    caption=message, 
                    parse_mode='MarkdownV2')
                
                
groups = ['os', 'gender']
sources = [extract_feed, extract_message]

dag = DAG(
    dag_id = 'anechaev_alert',
    schedule_interval='*/15 * * * *',
    catchup=False,
    default_args = default_args
)

tasks = []
for func in sources:
    for g in groups:
        tasks.append(
            PythonOperator(
                task_id=f'{func.__name__}_by_{g}'.replace(" ", "_"),
                python_callable=iqr_alert,
                op_kwargs={'groups': g, 'extract_func': func},
                dag=dag,
            )
        )

tasks

