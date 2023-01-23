import pandas as pd
from datetime import timedelta, datetime
import pandahouse as ph
import io
import telegram
from telegram import InputMediaPhoto, InputMediaDocument
import matplotlib.pyplot as plt
import seaborn as sns
plt.rcParams.update({'font.size': 22})

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

BOT_TOKEN = '<TOKEN>'
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

plot_kwargs = {
    'grid':True,
    'figsize': (12,8)
}

def transform_plot(metric_name,  x = 'dt', plot_type = 'line', **kwargs_plot):

    def real_decorator(func):
        def decorated(*args, **kwargs):
            
            df = func()
            df = df.set_index(x).sort_index()
    
            plot =df.plot 
            ax = getattr(plot, plot_type)(**kwargs_plot)
            ax.set_title(metric_name)

            plot_object = io.BytesIO()
            plot_object.name = metric_name
            ax.figure.savefig(plot_object)
            plot_object.seek(0)

            yd = df.iloc[-1].to_dict()
            text = f'*{metric_name}:*  \n ' + "  \n ".join(f'{k.replace("_", " ")}: {str(round(v,3)).replace(".",",")}' for k, v in yd.items())
            return {'text': text, 'plot': plot_object}
        return decorated
    return real_decorator


def transfrom_docs(file_name):
    def real_decorator(func):
        def decorated(*args, **kwargs):
            df = func()  
            file_object = io.StringIO()
            df.to_csv(file_object)
            file_object.name = file_name
            file_object.seek(0)
            return file_object
        return decorated
    return real_decorator



@dag(default_args = default_args, schedule_interval='0 8 * * *', catchup=False)
def full_report_anechaev():
    
    @task(task_id = 'extract_DAU')
    @transform_plot('DAU', plot_type='area', **plot_kwargs)
    def extract_DAU():
        q = """
            with t as (
            SELECT 
                if(dt = '1970-01-01', m.dt, dt) dt,
                user_id f_user_id,
                m.user_id m_user_id

            FROM (SELECT DISTINCT toDate(time) dt, user_id FROM {db}.feed_actions) f
            FULL OUTER JOIN ( SELECT DISTINCT toDate(time) dt, user_id FROM  {db}.message_actions) m
            on f.dt = m.dt and f.user_id =m.user_id

            )
            SELECT dt, 
                countIf(f_user_id, f_user_id!=0 and m_user_id=0) only_feed,
                countIf(m_user_id, f_user_id=0 and m_user_id!=0) only_message,
                countIf(f_user_id, f_user_id!=0 and m_user_id!=0) feed_message

            FROM t
            GROUP BY dt
            HAVING dt between today() -10 and yesterday()
            ORDER BY dt
        """
        return ph.read_clickhouse(q, connection=default_conn)
    
    @task(task_id = 'extract_ctr')
    @transform_plot('CTR', **plot_kwargs)
    def extract_ctr():
        q = """
            SELECT toDate(time) dt,
                  sum(action = 'like' and m.user_id=0)/ sum(action = 'view' and m.user_id=0) only_feed,
                  sum(action = 'like' and m.user_id!=0)/ sum(action = 'view' and m.user_id!=0) feed_message
            FROM {db}.feed_actions f
            LEFT JOIN
                (SELECT DISTINCT user_id,
                              toDate(time) dt,
                              exp_group
                FROM {db}.message_actions) m
            on toDate(f.time) = m.dt
            and f.user_id =m.user_id 
            GROUP BY dt
            HAVING dt between today() - 10 and yesterday()  
            ORDER BY dt
        """
        return ph.read_clickhouse(q, connection=default_conn)
    
    @task(task_id = 'num_actions_per_user')
    @transform_plot('Feed actions/AU', **plot_kwargs)
    def num_actions_per_user():
        q = """
            SELECT 
                toDate(time) dt,
               COUNT()/COUNT(DISTINCT user_id) num_actions_per_user
            FROM {db}.feed_actions
            GROUP BY dt
            HAVING dt between today() -10 and yesterday()
            ORDER BY dt
        """
        return ph.read_clickhouse(q, connection=default_conn)
    
    @task(task_id = 'num_messages_per_user')
    @transform_plot('Messages/AU', **plot_kwargs)
    def num_messages_per_user():
        q = """
            SELECT 
                toDate(time) dt,
                COUNT(reciever_id)/COUNT(DISTINCT user_id) num_messages_per_user
            FROM {db}.message_actions
            GROUP BY dt
            HAVING dt between today() -10 and yesterday()
            ORDER BY dt
        """
        return ph.read_clickhouse(q, connection=default_conn)
    
    @task(task_id = 'top_posts')
    @transfrom_docs(file_name='top_posts.csv')
    def top_posts():
        q = """
            SELECT post_id AS post_id,
               countIf(user_id, action ='view') AS "Views",
               countIf(user_id, action ='like') AS "Likes",
               countIf(user_id, action ='like')/countIf(user_id, action ='view') AS "CTR"
            FROM {db}.feed_actions
            WHERE time between today() -10 and yesterday()
            GROUP BY post_id
            ORDER BY "Views" DESC
            LIMIT 15;
        """
        return ph.read_clickhouse(q, connection=default_conn)
    
    @task(task_id = 'extract_dau_mau')
    @transform_plot('Stekiness', **plot_kwargs)
    def extract_dau_mau():

        q= """
            SELECT 
                toDate(time) dt,
                COUNT(DISTINCT user_id)/
             (SELECT count(DISTINCT user_id)
              FROM {db}.feed_actions
              WHERE toDate(time)> today() - 31 ) dau_mau

            FROM {db}.feed_actions
            GROUP BY dt
            HAVING dt between today() -10 and yesterday()
            ORDER BY dt
        """
        return ph.read_clickhouse(q, connection=default_conn)
    
    
    @task()
    def send(texts_plots, documents=None):
        
        texts, plots =zip(*[tp.values() for tp in texts_plots])
        
        dt = (datetime.today() -timedelta(days=1)).strftime('%Y\-%m\-%d') + '  \n'
        bot.sendMessage(chat_id=chat_id, text = f'*Full report {dt}*' + " \n".join(texts), parse_mode="MarkdownV2")
        
        objs = [InputMediaPhoto(p) for p in plots]

        bot.sendMediaGroup(chat_id=chat_id, media=objs)
        if documents:
            docs = [InputMediaDocument(d) for d in documents]
            bot.sendMediaGroup(chat_id=chat_id, media=docs)
    
    dau = extract_DAU()
    ctr = extract_ctr()
    napu = num_actions_per_user()
    nmpu = num_messages_per_user()
    stk = extract_dau_mau()
    tp = top_posts()

    send(texts_plots = [dau, ctr, napu, nmpu, stk] , documents = [tp])
    

full_report_anechaev = full_report_anechaev()
