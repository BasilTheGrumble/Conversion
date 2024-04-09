import random
import numpy.random
import pandas as pd
import numpy as np
from pandasql import sqldf



pd.set_option('display.width', None)


np.random.seed(54)
all_users_ids = np.arange(1, 1000)
all_apps_ids = np.arange(1, 5)

n = 10000

user_ids = np.random.choice(all_users_ids, n)
apps_ids = np.random.choice(all_apps_ids, n)

start_date = pd.to_datetime('2023-03-01')

times = pd.date_range(start_date, periods=n, freq='15min')

user_actions = pd.DataFrame({'userId': user_ids, 'product': np.nan, 'time': times})
user_actions['eventName'] = 'launch'


# Функция, генерирующая данные
def generate_user_actions(userId, product, time, reg_flag):
    register = 1
    page_visit = 0.3
    download = 0.1
    buy = 0.1
    update = 0.5

    df = pd.DataFrame()
    current_time = time
    product = int(np.random.choice(np.arange(1, 100), 1))
    if reg_flag == 1:

        if np.random.binomial(1, register, 1)[0]:
            df_register = pd.DataFrame({'userId': userId,
                                        'product': np.nan,
                                        'time': current_time + pd.Timedelta(10, 'm'),
                                        'eventName': 'register'}, index=[0])
            current_time += pd.Timedelta(10, 'm')

            if np.random.binomial(1, page_visit, 1)[0]:
                df_visit = pd.DataFrame({'userId': userId,
                                         'product': product,
                                         'time': current_time + pd.Timedelta(3, 'm'),
                                         'eventName': 'page_visit'}, index=[0])
                current_time += pd.Timedelta(3, 'm')

                if np.random.binomial(1, download, 1)[0]:
                    df_download = pd.DataFrame({'userId': userId,
                                                'product': product,
                                                'time': current_time + pd.Timedelta(3, 'm'),
                                                'eventName': 'download'}, index=[0])
                    current_time += pd.Timedelta(3, 'm')

                    if np.random.binomial(1, update, 1)[0]:
                        df_update = pd.DataFrame({'userId': userId,
                                                  'product': product,
                                                  'time': current_time + pd.Timedelta(15, 'D'),
                                                  'eventName': 'update'}, index=[0])
                        df_download = pd.concat([df_download, df_update])

                    df_visit = pd.concat([df_visit, df_download])

                else:
                    if np.random.binomial(1, buy, 1)[0]:
                        df_buy = pd.DataFrame({'userId': userId,
                                               'product': product,
                                               'time': current_time + pd.Timedelta(10, 'm'),
                                               'eventName': 'buy'}, index=[0])
                        current_time += pd.Timedelta(10, 'm')
                        df_buy = pd.concat([df_buy, pd.DataFrame({'userId': userId,
                                                                       'product': product,
                                                                       'time': current_time + pd.Timedelta(3, 'm'),
                                                                       'eventName': 'download'}, index=[0])])
                        current_time += pd.Timedelta(3, 'm')

                        if np.random.binomial(1, update, 1)[0]:
                            df_update = pd.DataFrame({'userId': userId,
                                                      'product': product,
                                                      'time': current_time + pd.Timedelta(15, 'D'),
                                                      'eventName': 'update'}, index=[0])
                            df_buy = pd.concat([df_buy, df_update])
                        df_visit = pd.concat([df_visit, df_buy])

                df_register = pd.concat([df_register, df_visit])

            df = pd.concat([df, df_register])
        return df

    else:
        if np.random.binomial(1, page_visit, 1)[0]:
            df_visit = pd.DataFrame({'userId': userId,
                                     'product': product,
                                     'time': current_time + pd.Timedelta(3, 'm'),
                                     'eventName': 'page_visit'}, index=[0])
            current_time += pd.Timedelta(3, 'm')

            if np.random.binomial(1, download, 1)[0]:
                df_download = pd.DataFrame({'userId': userId,
                                            'product': product,
                                            'time': current_time + pd.Timedelta(3, 'm'),
                                            'eventName': 'download'}, index=[0])
                current_time += pd.Timedelta(3, 'm')

                if np.random.binomial(1, update, 1)[0]:
                    df_update = pd.DataFrame({'userId': userId,
                                              'product': product,
                                              'time': current_time + pd.Timedelta(15, 'D'),
                                              'eventName': 'update'}, index=[0])
                    df_download = pd.concat([df_download, df_update])

                df_visit = pd.concat([df_visit, df_download])

            else:
                if np.random.binomial(1, buy, 1)[0]:
                    df_buy = pd.DataFrame({'userId': userId,
                                           'product': product,
                                           'time': current_time + pd.Timedelta(10, 'm'),
                                           'eventName': 'buy'}, index=[0])
                    current_time += pd.Timedelta(10, 'm')
                    df_buy = pd.concat([df_buy, pd.DataFrame({'userId': userId,
                                                              'product': product,
                                                              'time': current_time + pd.Timedelta(3, 'm'),
                                                              'eventName': 'download'}, index=[0])])
                    current_time += pd.Timedelta(3, 'm')

                    if np.random.binomial(1, update, 1)[0]:
                        df_update = pd.DataFrame({'userId': userId,
                                                  'product': product,
                                                  'time': current_time + pd.Timedelta(15, 'D'),
                                                  'eventName': 'update'}, index=[0])
                        df_buy = pd.concat([df_buy, df_update])
                    df_visit = pd.concat([df_visit, df_buy])

            df = pd.concat([df, df_visit])
        return df


final_df = pd.DataFrame()


set_of_unique_users = set()
for index, row in user_actions.iterrows():
    if row['userId'] not in set_of_unique_users:
        user_df = generate_user_actions(row['userId'], row['product'], row['time'], 1)
        final_df = pd.concat([final_df, user_df])
        set_of_unique_users.add(row['userId'])
    else:
        user_df = generate_user_actions(row['userId'], row['product'], row['time'], 0)
        final_df = pd.concat([final_df, user_df])
        set_of_unique_users.add(row['userId'])


user_actions = pd.concat([user_actions, final_df])
sqlsolution = user_actions.copy()
third_task = user_actions.copy()


# user_actions = df  если изначально дан df, то можно сделать так

# Второе задание
user_actions = user_actions.sort_values(by='time')

# выделяем номер недели из даты
# Важный момент! Выделяются номера недель в году. Это значит, что на следующий год номера недель будут те же самые
# и при таком решении смешаются данные за разные годы - лучше выделять не только номер недели, но и год. Но, следуя
# условию задания, опустим это.
user_actions['week'] = user_actions['time'].dt.isocalendar().week


# проходимся по значениям событий - ищем регистрации и загрузки, создаём массив флагов
registration = []
download = []
for index, row in user_actions.iterrows():
    if row['eventName'] == 'register':
        registration.append(1)
    else:
        registration.append(0)
    if row['eventName'] == 'download':
        download.append(1)
    else:
        download.append(0)

user_actions['isRegistrated'] = registration
user_actions['isDownloaded'] = download


# группируем по каждому пользователю и по номеру недели, считая регистрации (не бывает больше 1)
# и загрузки для каждой строчки в группировке
by_weeks = user_actions.groupby(by=['userId', 'week']).agg({'isRegistrated': 'sum', 'isDownloaded': 'sum'})\
                       .reset_index()


# создаём итоговый массив флагов, отмечаем совпадения: на неделе совершена регистрация и загрузка (возможно, не одна)
goal_flag = []
for index, row in by_weeks.iterrows():
    if row['isRegistrated'] == 1 and row['isDownloaded'] >= 1:
        goal_flag.append(1)
    else:
        goal_flag.append(0)


by_weeks['goal_flag'] = goal_flag


# отбираем только те записи (недели), где была совершена загрузка, так как нас не интересуют записи без регистрации
by_weeks = by_weeks.loc[by_weeks['isRegistrated'] == 1]


# группируем полученную на данный момент таблицу по неделям, считая сначала сумму зарегестрировавшихся и загрузивших
# а затем количество уникальных пользователей для недели
# хоть и создаётся два датафрейма из одной колонки, но у них одинаковая размерность
by_weeks_sum = by_weeks.groupby('week').agg({'goal_flag': 'sum'}).rename(columns={'goal_flag': 'reg&download_users'})\
                       .reset_index()
by_weeks_count = by_weeks.groupby('week').agg({'userId': 'count'}).rename(columns={'userId': 'amount_of_users'})\
                         .reset_index()


# соединяем датафреймы по номеру недели
answer = pd.merge(by_weeks_sum, by_weeks_count, on='week')

# считаем конверсию с округлением до 2-х знаков после запятой (не в процентах!)
answer['CR'] = round(answer['reg&download_users']/answer['amount_of_users'], 2)

# переименовываем колонку для соответствия выводу в заданиии
answer = answer.rename(columns={'amount_of_users': 'users'})

# отбираем нужные колонки
answer = answer[['week', 'users', 'CR']]




# SQL запрос для подсчёта конверсии в загрузку на неделе регистрации
q = """ WITH cte1 AS (
            SELECT 
                userId,
                strftime('%W',time) AS week,
                CASE WHEN eventName = 'register' THEN 1 ELSE 0 END AS isRegistrated,
                CASE WHEN eventName = 'download' THEN 1 ELSE 0 END AS isDownloaded
            FROM sqlsolution),
        cte2 AS (   
            SELECT 
                userId,
                week,
                SUM(isRegistrated) AS isRegistratedSum,
                SUM(isDownloaded) AS isDownloadedSUM
            FROM cte1
            GROUP BY userId, week),
        cte3 AS (
            SELECT
                *,
                CASE WHEN isRegistratedSum = 1 AND isDownloadedSUM >= 1 THEN 1 ELSE 0 END AS goal_flag
            FROM cte2
            WHERE isRegistratedSum = 1)
        SELECT 
            week,
            COUNT(DISTINCT userId) AS reg,
            ROUND((1.0*SUM(goal_flag))/COUNT(DISTINCT userId), 2) AS "CR"
        FROM cte3
        GROUP BY week;"""

ans = sqldf(q)

# вывод одинаковых результатов Python и SQL скриптов
print(ans)
print(answer)
