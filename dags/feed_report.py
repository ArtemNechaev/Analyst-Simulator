import pandas as pd
from datetime import timedelta
import pandahouse as ph
import io
import telegram
import matplotlib.pyplot as plt
import seaborn as sns
plt.rcParams.update({'font.size': 22})

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

BOT_TOKEN = '5892789921:AAGHn5cInwBv7V0QkfSZgN2fotTYzId5QSA'
chat_id = -817095409

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

@dag(default_args = default_args, schedule_interval='55 7 * * *', catchup=False)
def feed_report_anechaev(): 
    
    @task()
    def extract():
        q= f"""
            SELECT 
                toDate(time) dt, 
                count(DISTINCT user_id) DAU,
                SUM(action ='like') likes,
                SUM(action ='view') views,
                likes/views ctr

            FROM {default_conn['database']}.feed_actions
            WHERE dt between today() - 7 and today() - 1
            GROUP BY dt
            ORDER BY dt
            
        """
        return ph.read_clickhouse(q, connection=default_conn)
    
    @task()
    def send_report(df):
        columns = df.columns.drop('dt').tolist()
        fig, axs = plt.subplots(len(columns),1, figsize=(12,12))
        colors = ['red', 'orange', 'green', '#235789']

        for i, (column, color) in enumerate(zip(columns, colors)):
            axs[i].plot(df['dt'], df[column], color=color)
            axs[i].set_title(column)
            axs[i].grid()
            if len(columns) > i + 1:
                axs[i].set_xticklabels([])
                axs[i].set_xlabel('')
            else:
                plt.xticks(rotation=20)

        plot_object = io.BytesIO()
        plot_object.name = ''
        fig.savefig(plot_object)
        plot_object.seek(0)

        yd = df.iloc[-1]
        dt = yd["dt"].strftime("%Y\-%m\-%d")
        bot.sendMessage(chat_id=chat_id, 
                    text=f'*Feed report {dt}*  \n' +\
                        f'👦👩*DAU:* {yd.DAU}  \n' +\
                        f' ❤️   *Likes*: {yd.likes}  \n' +\
                        f' 👀   *Views:* {yd.views}  \n' +\
                        f'❤️/👀 *CTR:* {str(round(yd.ctr, 2)).replace(".",",")}', 
                    parse_mode='MarkdownV2')

        bot.sendPhoto(chat_id = chat_id, photo = plot_object)
    
    df = extract()
    send_report(df)

feed_report_anechaev = feed_report_anechaev()
