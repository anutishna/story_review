import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from data_processing import count_eps_ea_by_day, update_progress, count_cumulation
from db_connection import db_query_dataframe, read_query, update_query
from data_visualization import published_eps_trace, ea_markers_trace
import datetime
import plotly.express as px
import pandas as pd

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

DB_SETTINGS = 'host=localhost dbname=addicted \
    user=addicted_r password=addicted_r port=3456'


def get_df_from_db(query_path, story):
    """Функция для выполнения SQL-запроса, расположенного по указанному адресу."""
    query = read_query(query_path)
    query = update_query(query, params={'story_id': story})
    return db_query_dataframe(DB_SETTINGS, query)


def get_story_ids_by_date(start_date, end_date):
    """Функция для выявления списка идентификаторов историй, опубликованных за определенный период."""
    query = read_query('sqls/get_story_ids_by_date.sql')
    query = update_query(query, params={'start_date': start_date, 'end_date': end_date})
    df = db_query_dataframe(DB_SETTINGS, query)
    story_ids = df['story_id']
    return df, story_ids


def get_story_id_by_title(title):
    """Функция для выявления идентификатора истории по названию"""
    query = read_query('sqls/get_story_id_by_title.sql')
    query = update_query(query, params={'title': title})
    df = db_query_dataframe(DB_SETTINGS, query)
    if df.empty:
        print('История с названием "{}" не найдена.'.format(title))
        return ''
    else:
        return df['story_id'][0]


def get_series_ids():
    query = read_query('sqls/get_series.sql')
    query = update_query(query)
    df = db_query_dataframe(DB_SETTINGS, query)
    return df


def get_story_info(story):
    df = get_df_from_db('sqls/get_story_info.sql', story)
    title = df['title'][0]
    eps = df['episodesCount'][0]
    pub_date = df['releasedAt'][0]
    return title, eps, pub_date


def preprocess_story_data(story):
    progress = get_df_from_db('sqls/get_progress.sql', story)
    purchases = get_df_from_db('sqls/get_purchases.sql', story)
    episodes = get_df_from_db('sqls/get_episodes.sql', story)

    episodes = episodes[(episodes['published_at'] <= datetime.date.today())]
    episodes = episodes[(episodes['published_at'].isnull() == False)]

    ep_info = count_eps_ea_by_day(episodes)
    progress = update_progress(progress, episodes)
    zero_eps_read = progress[(progress['fin_eps']) == '0']
    ten_eps_read = progress[(progress['fin_eps']) == '10']

    purs = count_cumulation(purchases, 'total_revenue', 'sum_revenue')
    purs = count_cumulation(purchases, 'early_access_revenue', 'sum_ea_revenue')
    purs = count_cumulation(purchases, 'ios_subs_revenue', 'sum_ios_subs_revenue')
    purs = count_cumulation(purchases, 'android_subs_revenue', 'sum_android_subs_revenue')

    ep_info = ep_info[(ep_info['date'] <= datetime.date.today())]

    return progress, episodes, ep_info, zero_eps_read, ten_eps_read, purs


def visualize_story(progress, episodes, ep_info, zero_eps_read, ten_eps_read, purs):
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(go.Scatter(
        x=purs['purchaseDate'],
        y=purs['sum_ea_revenue'],
        name='EA revenue',
        legendgroup='revenue',
        stackgroup='one', mode='none'
    ), secondary_y=True)

    fig.add_trace(go.Scatter(
        x=purs['purchaseDate'],
        y=purs['sum_ios_subs_revenue'],
        name='iOS subs revenue',
        legendgroup='revenue',
        stackgroup='one', mode='none'
    ), secondary_y=True)

    fig.add_trace(go.Scatter(
        x=purs['purchaseDate'],
        y=purs['sum_android_subs_revenue'],
        name='Android subs revenue',
        legendgroup='revenue',
        stackgroup='one', mode='none'
    ), secondary_y=True)

    fig.add_trace(go.Scatter(
        x=progress['date'],
        y=progress['users_stop_reading'],
        name="users stopped reading",
        legendgroup='read'
    ), secondary_y=False)

    fig.add_trace(go.Scatter(
        x=zero_eps_read['date'],
        y=zero_eps_read['users_read'],
        name="users stopped at 0 ep",
        legendgroup='read'
    ), secondary_y=False)

    fig.add_trace(go.Scatter(
        x=ten_eps_read['date'],
        y=ten_eps_read['users_read'],
        name="users finished story",
        legendgroup='read'
    ), secondary_y=False)

    fig.add_trace(published_eps_trace(
        episodes, zero_eps_read['users_read'].min()
    ), secondary_y=False)

    fig.add_trace(ea_markers_trace(
        ep_info, progress['users_stop_reading'].max()
    ), secondary_y=False)

    fig.update_layout(
        title=episodes['title'][0],
        height=600,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )

    fig.update_yaxes(title_text="users", secondary_y=False)
    fig.update_yaxes(title_text="₽", secondary_y=True)

    return fig


def visualize_stories_by_dates(start_date, end_date):
    _, story_ids = get_story_ids_by_date(start_date=start_date, end_date=end_date)
    fig_rev = make_subplots()
    no_purchased_stories = []

    print('Total stories:', len(story_ids))

    for story_id in story_ids:
        print('Getting data for story {}...'.format(story_id))
        _, episodes_s, _, _, _, purs_s = preprocess_story_data(story_id)
        if not purs_s.empty:
            fig_rev.add_trace(go.Scatter(
                x=purs_s['purchaseDate'],
                y=purs_s['sum_revenue'],
                name=episodes_s['title'][0]
            ))
        else:
            no_purchased_stories.append(episodes_s['title'][0])

    fig_rev.update_layout(
        title='Stories Revenue',
        height=600,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    fig_rev.update_yaxes(title_text="₽")

    return fig_rev, no_purchased_stories


def text_to_date(input_date):
    date_list = input_date.split('-')

    year = int(date_list[0])

    if date_list[1].startswith('0'):
        month = int(date_list[1][1:])
    else:
        month = int(date_list[1])

    if date_list[2].startswith('0'):
        day = int(date_list[2][1:])
    else:
        day = int(date_list[2])
    return datetime.date(year, month, day)


def process_data(story):
    df = get_df_from_db('sqls/get_retention_info.sql', story)
    title, story_length, pub_date = get_story_info(story)

    df = df[(df['fin_eps'] <= story_length)]
    df = df[(df['read_at'] >= pub_date)]

    df['days_before_read'] = df['read_at'] - df['created_at']
    df['days_before_read'] = df['days_before_read'].apply(lambda x: x.days)
    df['days_after_read'] = df['last_active_at'] - df['read_at']
    df['days_after_read'] = df['days_after_read'].apply(lambda x: x.days)
    df['total_exist_days'] = df['last_active_at'] - df['created_at']
    df['total_exist_days'] = df['total_exist_days'].apply(lambda x: x.days)

    filtered_ret_df = df[(df['days_after_read'] >= 0)]
    filtered_ret_df = filtered_ret_df[(filtered_ret_df['days_before_read'] > 0)]

    filtered_ret_df['before_read_share'] = filtered_ret_df['days_before_read'] / filtered_ret_df['total_exist_days']
    filtered_ret_df['after_read_share'] = filtered_ret_df['days_after_read'] / filtered_ret_df['total_exist_days']

    #     filtered_ret_df = filtered_ret_df[(
    #         filtered_ret_df['last_active_at'] < datetime.date.today() - datetime.timedelta(days=7))]
    return filtered_ret_df, title, pub_date


def get_churn_rate(story):
    filtered_ret_df, title, pub_date = process_data(story)
    input_churn_df = filtered_ret_df[['user_id', 'days_after_read']].groupby('days_after_read').count().reset_index()
    input_read_df = filtered_ret_df[['user_id', 'fin_eps']].groupby('fin_eps').count().reset_index()

    churn_df = []
    sum_users = input_churn_df['user_id'].sum()
    prev_users = 0

    for n, row in input_churn_df.iterrows():
        row['sum_users'] = sum_users
        row['churn'] = row['user_id'] / sum_users
        row['churn_from_prev'] = row['user_id'] / (sum_users - prev_users)
        row['story'] = title
        prev_users += row['user_id']

        churn_df.append(row.values)

    story_churn_data = pd.DataFrame(
        churn_df, columns=['days_after_read', 'user_id', 'sum_users', 'churn', 'churn_from_prev', 'story'])

    churn_df = []
    sum_users = input_read_df['user_id'].sum()
    prev_users_sum = 0
    prev_users = sum_users

    for n, row in input_read_df.iterrows():
        row['users'] = sum_users - prev_users_sum
        row['churn'] = row['users'] / sum_users
        row['churn_from_prev'] = row['users'] / prev_users
        row['story'] = title

        prev_users_sum += row['user_id']
        prev_users = row['users']
        churn_df.append(row.values)

    story_read_data = pd.DataFrame(churn_df, columns=[
        'fin_eps', 'user_id', 'users', 'churn', 'churn_from_prev', 'story'])

    return story_churn_data, story_read_data


def series_churn_fig():
    df = get_series_ids()

    churn_rate_df = []
    churn_read_rate_df = []

    for n, row in df.iterrows():
        churn_rate, read_rate = get_churn_rate(row['story_id'])[:-7]
        churn_rate_df.append(churn_rate)
        churn_read_rate_df.append(read_rate)

    churn_data = pd.concat(churn_rate_df, sort=False, ignore_index=True)
    churn_read_data = pd.concat(churn_read_rate_df, sort=False, ignore_index=True)

    fig_churn = px.line(
        churn_data,
        x="days_after_read",
        y="churn_from_prev",
        color="story",
        title='Churn After Story Read')

    fig_read_churn = px.line(
        churn_read_data,
        x="fin_eps",
        y="churn_from_prev",
        color="story",
        title='Churn After Read Episode')
    return fig_churn, fig_read_churn


# churn_fig, read_fig = series_churn_fig()


app.layout = html.Div(children=[
    html.H1(children='Story Review'),

    html.Div(
        dcc.Graph(
            id='single_story_review',
        ),
        style={"width": "60%", "float": "right"},
    ),

    html.Div(
        dcc.Graph(
            id='multiple_stories_revenue',
        ),
        style={"width": "40%", "float": "left"},
    ),

    html.Div(["Story Title: ",
              dcc.Input(id='story_title', value='Изоляция', type='text')],
             style={"width": "60%", "float": "right"},
             ),

    html.Div(["Start Date: ",
              dcc.Input(id='start_date', value='2020-09-07', type='text')],
             style={"width": "20%", "float": "left"},
             ),

    html.Div(["End Date: ",
              dcc.Input(id='end_date', value='2020-09-13', type='text')],
             style={"width": "20%", "float": "left"},
             ),

])


@app.callback(
    Output('single_story_review', 'figure'),
    [Input('story_title', 'value')]
)
def update_graph(story_title):
    story_id = get_story_id_by_title(story_title)

    if story_id == '':
        raise PreventUpdate

    progress, episodes, ep_info, zero_eps_read, ten_eps_read, purs = preprocess_story_data(
        story_id
    )
    fig = visualize_story(
        progress, episodes, ep_info, zero_eps_read, ten_eps_read, purs
    )
    return fig


@app.callback(
    Output('multiple_stories_revenue', 'figure'),
    [
        Input('start_date', 'value'),
        Input('end_date', 'value')
    ]
)
def update_multiple_graph(start_date_text, end_date_text):
    if len(start_date_text) < 10 or len(end_date_text) < 10:
        raise PreventUpdate

    start_date = text_to_date(start_date_text)
    end_date = text_to_date(end_date_text)

    fig_rev, _ = visualize_stories_by_dates(start_date, end_date)
    return fig_rev


if __name__ == '__main__':
    app.run_server(debug=True)
