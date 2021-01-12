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


def get_ads_revenue(progress):
    """Получает информацию о доходах с рекламы"""
    ads_rev = pd.read_csv('ads_revenue.csv')

    temp_p = progress.copy()
    temp_p['fin_eps'] = temp_p['fin_eps'].apply(lambda x: int(x))
    temp_p['eps_read'] = temp_p['fin_eps'] * temp_p['users_read']
    temp_p = temp_p[['date', 'eps_read']].groupby('date').sum().reset_index()
    temp_p['period'] = temp_p['date'].apply(lambda x: x.strftime("%B") + ' ' + str(x.year))

    ads_rev_progress = temp_p.merge(ads_rev, left_on='period', right_on='Period ', how='outer')[[
        'date', 'eps_read', 'Rev per Episode'
    ]]
    ads_rev_progress = ads_rev_progress[(ads_rev_progress['date'].isna() == False)].drop_duplicates()
    ads_rev_progress['ads_revenue'] = ads_rev_progress['eps_read'] * ads_rev_progress['Rev per Episode']
    ads_rev_progress = ads_rev_progress.fillna(0)
    count_cumulation(ads_rev_progress, 'ads_revenue', 'sum_ads_revenue')
    return ads_rev_progress


def preprocess_story_data(story):
    progress = get_df_from_db('sqls/get_progress.sql', story)
    purchases = get_df_from_db('sqls/get_purchases.sql', story)
    episodes = get_df_from_db('sqls/get_episodes.sql', story)

    episodes = episodes[(episodes['published_at'] <= datetime.date.today())]
    episodes = episodes[(episodes['published_at'].isnull() == False)]

    ep_info = count_eps_ea_by_day(episodes)
    progress = update_progress(progress, episodes)
    #     progress = count_cumulation(progress, '')
    #     zero_eps_read = progress[(progress['fin_eps']) == '0']
    #     ten_eps_read = progress[(progress['fin_eps']) == '10']

    if not purchases.empty:
        base = purchases.purchaseDate[0]
        numdays = (datetime.date.today() - base).days
        date_list = [base + datetime.timedelta(days=x) for x in range(numdays + 1)]
        data = {'purchaseDate': date_list}
        dates_df = pd.DataFrame(data)
        purchases = dates_df.merge(purchases, on='purchaseDate', how='outer').fillna(0)
    else:
        purchases = pd.DataFrame(columns=['purchaseDate', 'paid_users', 'all_purs', 'ios_subs', 'ios_rebills',
                                          'android_subs', 'android_rebills', 'total_revenue', 'ios_subs_revenue',
                                          'android_subs_revenue', 'early_access_revenue'])

    count_cumulation(purchases, 'total_revenue', 'sum_revenue')
    count_cumulation(purchases, 'early_access_revenue', 'sum_ea_revenue')
    count_cumulation(purchases, 'ios_subs_revenue', 'sum_ios_subs_revenue')
    count_cumulation(purchases, 'android_subs_revenue', 'sum_android_subs_revenue')

    ep_info = ep_info[(ep_info['date'] <= datetime.date.today())]

    ads_revenue = get_ads_revenue(progress)

    zero_eps_read = progress[progress.fin_eps == 0][['date', 'users_read']]
    count_cumulation(zero_eps_read, 'users_read', 'sum_users_read')

    max_eps_read = progress[progress.fin_eps == episodes.episodesCount[0]][['date', 'users_read']]
    count_cumulation(max_eps_read, 'users_read', 'sum_users_read')

    progress_rev = progress[['date', 'users_stop_reading', 'ios_users_stop_reading',
                             'android_users_stop_reading']].merge(
        purchases[['purchaseDate', 'sum_ios_subs_revenue', 'sum_android_subs_revenue', 'sum_ea_revenue',
                   'sum_revenue']],
        left_on='date', right_on='purchaseDate', how='outer'
    ).drop(columns='purchaseDate').merge(
        ads_revenue[['date', 'sum_ads_revenue']], on='date'
    ).drop_duplicates()

    count_cumulation(progress_rev, 'users_stop_reading', 'sum_users_stop_reading')
    count_cumulation(progress_rev, 'ios_users_stop_reading', 'sum_ios_users_stop_reading')
    count_cumulation(progress_rev, 'android_users_stop_reading', 'sum_android_users_stop_reading')

    return episodes, ep_info, zero_eps_read, max_eps_read, progress_rev


def column_to_float(df, col_name):
    df[col_name] = df[col_name].apply(lambda x: float(x))


def get_revenue_per_platform(story_ids):
    total_stories = len(story_ids)
    print('Total stories:', total_stories)
    i = 1

    rev_data_list = []
    for id in story_ids:
        print('Getting data for story {}/{}...'.format(i, total_stories))
        episodes, _, _, _, progress_rev = preprocess_story_data(id)
        rev_data = progress_rev.drop(columns=['date', 'users_stop_reading', 'ios_users_stop_reading',
                                              'android_users_stop_reading']).fillna(0).reset_index(drop=True).iloc[-1:]

        title = episodes.title[0]
        rev_data['title'] = title
        rev_data_list.append(rev_data)

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

    ios_data['rev_per_user'] = ios_data.subscribtions / ios_data.users

    column_to_float(and_data, 'subscribtions')
    column_to_float(and_data, 'early_accesses')
    and_data['rev_per_user_no_ea'] = (and_data.ads + and_data.subscribtions) / and_data.users
    and_data['rev_per_user_with_ea'] = (and_data.ads + and_data.subscribtions +
                                        and_data.early_accesses) / and_data.users
    return ios_data, and_data
