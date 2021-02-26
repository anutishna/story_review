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


def append_ads_revenue(df):
    cols = list(df.columns)
    ads_rev = pd.read_csv('ads_revenue.csv')
    df['period'] = df['updated_at'].apply(lambda x: x.strftime("%B") + ' ' + str(x.year))
    df = df.merge(ads_rev, left_on='period', right_on='Period ', how='left')
    df = df.fillna(0)
    df['ads_revenue'] = df['and_eps_read'] * df['Rev per Episode']
    count_cumulation(df, 'ads_revenue', 'ads_revenue_cum')
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
        episodes, _, data = preprocess_story_data(s_id)
        rev_data = data[['and_users_stopped_cum', 'ios_users_stopped_cum',
                         'ios_subs_revenue_cum',
                         'android_subs_revenue_cum', 'ads_revenue_cum', 'early_access_revenue_cum']].iloc[-1:]

        title = episodes.title[0]
        rev_data['title'] = title
        rev_data_list.append(rev_data)
        i += 1

    rev_data_df = pd.concat(rev_data_list)

    for col in rev_data_df.columns:
        if col != 'title':
            column_to_float(rev_data_df, col)

    rev_data_df['ios_rev_per_user'] = rev_data_df['ios_subs_revenue_cum'] / rev_data_df['ios_users_stopped_cum']
    rev_data_df['and_rev_per_user'] = (rev_data_df['android_subs_revenue_cum']
                                       + rev_data_df['ads_revenue_cum']) / rev_data_df['and_users_stopped_cum']
    rev_data_df['and_rev_per_user_ea'] = (rev_data_df['android_subs_revenue_cum']
                                          + rev_data_df['ads_revenue_cum']
                                          + rev_data_df['early_access_revenue_cum']) / rev_data_df['and_users_stopped_cum']
    return rev_data_df
