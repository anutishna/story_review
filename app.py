import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from data_processing import count_eps_ea_by_day, update_progress, count_cumulation
from db_connection import db_query, db_query_dataframe, read_query, update_query
from data_visualization import published_eps_trace, ea_markers_trace

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

DB_SETTINGS = 'host=localhost dbname=addicted \
    user=addicted_r password=addicted_r port=3456'


def get_df_from_db(query_path, story):
    """Функция для выполнения SQL-запроса, расположенного по указанному адресу."""
    query = read_query(query_path)
    query = update_query(query, params={'story_id' : story})
    return db_query_dataframe(DB_SETTINGS, query)


def preprocess_story_data(story):
    progress = get_df_from_db('sqls/get_progress.sql', story)
    purchases = get_df_from_db('sqls/get_purchases.sql', story)
    episodes = get_df_from_db('sqls/get_episodes.sql', story)

    ep_info = count_eps_ea_by_day(episodes)
    progress = update_progress(progress, episodes)
    zero_eps_read = progress[(progress['fin_eps']) == '0']
    ten_eps_read = progress[(progress['fin_eps']) == '10']

    purs = count_cumulation(purchases, 'total_revenue', 'sum_revenue')
    purs = count_cumulation(purchases, 'early_access_revenue', 'sum_ea_revenue')
    purs = count_cumulation(purchases, 'ios_subs_revenue', 'sum_ios_subs_revenue')
    purs = count_cumulation(purchases, 'android_subs_revenue', 'sum_android_subs_revenue')

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


app.layout = html.Div(children=[
    html.H1(children='Story Review'),

    html.Div(children='''
        Shows the way story is being published, read and purchased during the timeline.
    '''),

    dcc.Graph(
        id='my-output',
    ),

    html.Div(["Input: ",
              dcc.Input(id='my-input', value='j2YTBTDFI1', type='text')])

])

@app.callback(
    Output(component_id='my-output', component_property='figure'),
    [Input(component_id='my-input', component_property='value')]
)
def update_graph(input_value):
    progress, episodes, ep_info, zero_eps_read, ten_eps_read, purs = preprocess_story_data(
        input_value
    )

    fig = visualize_story(
        progress, episodes, ep_info, zero_eps_read, ten_eps_read, purs
    )
    return fig

if __name__ == '__main__':
    app.run_server(debug=True)