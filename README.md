## analyst_simulator

![AnalystSimulator](https://user-images.githubusercontent.com/62764290/216961922-3e2675ce-b3a3-4e8c-b0a0-4a837c2e4e5d.PNG)
Данный репозиторий является результатом стажировки по программе "Симулятор аналитика" karpov.cources.

Разработал дашборды для анализа основных метрик с использованием BI системы Superset
![feed](https://github.com/ArtemNechaev/Analyst-Simulator/blob/main/feed-dash.jpg)

Выполнен анализ продуктовых метрик:
- сравнение retention органического и рекламного трафика
- проведен анализ эффекта от крупной рекламной компании на количество новых пользователей
- найдены причины резкого снижения аудитории в конкретную дату
- построен график активной аудитории с разбиением на новых, старых и ушедших пользователей

Провел эксперимент по влиянию новой системы рекомендаций на метрику CTR:
- проверка системы разделения на группы - AA-test
- проведение A/B теста
- выводы по результатам эксперимента.

Разработал ETL pipeline для агрегации основных метрик мессенджера и ленты новостей в разрезе пола, возраста и OS.

Автоматизировал отёчность по основным метрикам ленты новостей и мессенджера с использованием Airflow и Telegram.

Разработал систему оповещения в Telegram об аномалиях в метриках ленты новостей и мессенджера. Доверительный интервал метрик определяется по межквартильному размаху.

