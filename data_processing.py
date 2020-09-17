import pandas as pd
import numpy as np
import datetime


def get_first_ep_date(episodes_df):
    """Функция для определения даты публикации истории"""
#     print(episodes_df['published_at'].min())
    return episodes_df['published_at'].min()


def get_last_ep_date(episodes_df):
    """Функция для определения даты публикации последнего эпизода
    или даты наступления бесплатного доступа ко всем эпизодам –
    в зависимости от того, что наступит позже."""
    free_access = episodes_df['free_access_at'].dropna()

    if free_access.empty:
#         print(episodes_df['published_at'].max())
        return episodes_df['published_at'].max()
    else:
#         print(max(episodes_df['published_at'].max(),
#                    free_access.max()))
        return max(episodes_df['published_at'].max(),
                   free_access.max())


def get_pub_days_num(episodes_df):
    """Функция для подсчета количества дней, в течение которых
    публиковалась история."""
    return (
            get_last_ep_date(episodes_df) - get_first_ep_date(episodes_df)
    ).days


def update_progress(progress_df, episodes_df):
    """Функция для корректирования таблицы с прогрессом.
    Отбрасывает мусорные даты, а также считает сколько обновлений прогресса было в каждую дату."""
    story_published_at = get_first_ep_date(episodes_df)

    progress_df = progress_df[(progress_df['updated_at'] <= datetime.date.today())]
    progress_df = progress_df[(
            progress_df['updated_at'] >= story_published_at)].reset_index(drop=True)

    total_start_read_data = progress_df[['updated_at', 'users_read']].groupby(
        'updated_at').sum().reset_index().rename(
        columns={'users_read': 'users_stop_reading'})

    return progress_df.merge(
        total_start_read_data, on='updated_at',
        how='outer').rename(columns={'updated_at': 'date'})


def count_eps_ea_by_day(episodes_df):
    """Функция для подсчета логов: сколько эпизодов опубликовано, а сколько – находятся на раннем доступе
    по датам на протяжении всех дней, в течение которых история публиковалась."""
    rows = []
    eps_published = 0
    pub_days = get_pub_days_num(episodes_df)
    first_ep = get_first_ep_date(episodes_df)

    for i in range(pub_days + 1):
        curr_day = first_ep + datetime.timedelta(days=i)
        ea_count = 0

        for _, row in episodes_df.iterrows():
            if curr_day == row['published_at']:
                eps_published = row['ep_num']

            if pd.isnull(row['free_access_at']):
                continue
            elif row['published_at'] <= curr_day < row['free_access_at']:
                ea_count += 1

        row = {
            'date': curr_day,
            'eps_published': eps_published,
            'ea_count': ea_count
        }
        rows.append(row)

    return pd.DataFrame.from_dict(rows)


def count_cumulation(df, col_name, new_col_name):
    count = 0
    col_list = []

    for _, row in df.iterrows():
        count += row[col_name]
        col_list.append(count)
    df[new_col_name] = col_list
    return df