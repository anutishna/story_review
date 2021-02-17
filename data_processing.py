import pandas as pd
import datetime
from data_getter import get_df_from_db


def get_first_ep_date(episodes_df):
    """Функция для определения даты публикации истории"""
    return episodes_df['published_at'].min()


def get_last_ep_date(episodes_df):
    """Функция для определения даты публикации последнего эпизода
    или даты наступления бесплатного доступа ко всем эпизодам –
    в зависимости от того, что наступит позже."""
    free_access = episodes_df['free_access_at'].dropna()

    if free_access.empty:
        return episodes_df['published_at'].max()
    else:
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

    total_start_read_data = progress_df[['updated_at', 'users_read', 'ios_users_read', 'android_users_read']].groupby(
        'updated_at').sum().reset_index().rename(
        columns={'users_read': 'users_stop_reading', 'ios_users_read': 'ios_users_stop_reading',
                 'android_users_read': 'android_users_stop_reading'})

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
    # return df


def append_ads_revenue(df):
    cols = list(df.columns)
    ads_rev = pd.read_csv('ads_revenue.csv')
    df['period'] = df['updated_at'].apply(lambda x: x.strftime("%B") + ' ' + str(x.year))
    df = df.merge(ads_rev, left_on='period', right_on='Period ', how='left')
    df['ads_revenue'] = df['and_eps_read'] * df['Rev per Episode']
    count_cumulation(df, 'ads_revenue', 'ads_revenue_cum')
    df = df.fillna(0)
    cols.extend(['ads_revenue', 'ads_revenue_cum'])
    return df[cols]


def preprocess_story_data(story):
    print('Getting progress and purchases data from DB...')
    data = get_df_from_db('sqls/get_progs_and_purs_data.sql', story)
    print('Getting story info from DB...')
    episodes = get_df_from_db('sqls/get_episodes.sql', story)

    episodes = episodes[(episodes['published_at'] <= datetime.date.today())]
    episodes = episodes[(episodes['published_at'].isnull() == False)]

    ep_info = count_eps_ea_by_day(episodes)

    ep_info = ep_info[(ep_info['date'] <= datetime.date.today())]

    data = append_ads_revenue(data)
    return episodes, ep_info, data


def column_to_float(df, col_name):
    df[col_name] = df[col_name].apply(lambda x: float(x))


def get_revenue_per_platform(story_ids):
    total_stories = len(story_ids)
    print('Total stories:', total_stories)
    i = 1

    rev_data_list = []
    for s_id in story_ids:
        print('Getting data for story {}/{}...'.format(i, total_stories))
        episodes, _, _, _, progress_rev = preprocess_story_data(s_id)
        rev_data = progress_rev.drop(columns=['date', 'users_stop_reading', 'ios_users_stop_reading',
                                              'android_users_stop_reading']).fillna(0).reset_index(drop=True).iloc[-1:]

        title = episodes.title[0]
        rev_data['title'] = title
        rev_data_list.append(rev_data)
        i += 1

    rev_data_df = pd.concat(rev_data_list)

    ios_data = rev_data_df[['title', 'sum_ios_users_stop_reading', 'sum_ios_subs_revenue']]
    and_data = rev_data_df[['title', 'sum_android_users_stop_reading',
                            'sum_android_subs_revenue', 'sum_ea_revenue', 'sum_ads_revenue']]
    ios_data = ios_data.rename(columns={'sum_ios_users_stop_reading': 'users',
                                        'sum_ios_subs_revenue': 'subscribtions'}).reset_index(drop=True)
    and_data = and_data.rename(columns={'sum_android_users_stop_reading': 'users',
                                        'sum_android_subs_revenue': 'subscribtions',
                                        'sum_ea_revenue': 'early_accesses',
                                        'sum_ads_revenue': 'ads'}).reset_index(drop=True)

    column_to_float(ios_data, 'subscribtions')
    ios_data['rev_per_user'] = ios_data.subscribtions / ios_data.users

    column_to_float(and_data, 'subscribtions')
    column_to_float(and_data, 'early_accesses')
    and_data['rev_per_user_no_ea'] = (and_data.ads + and_data.subscribtions) / and_data.users
    and_data['rev_per_user_with_ea'] = (and_data.ads + and_data.subscribtions +
                                        and_data.early_accesses) / and_data.users
    return ios_data, and_data
