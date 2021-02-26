import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

from data_getter import get_story_id_by_title, get_story_ids_by_date
from data_visualization import visualize_story, vis_revenue_per_platfrom
from data_processing import preprocess_story_data, get_revenue_per_platform

import datetime

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(children=[
    html.H1(children='Story Review'),

    html.Div(["Story Title: ",
              dcc.Input(id='story_title', value='Ученица чародея', type='text')],
             style={"width": "100%", "float": "left"},
             ),

    html.Div(
        dcc.Graph(
            id='single_story_review',
        ),
        style={"width": "100%", "height": "100%", "float": "left"},
    ),

    html.H1(children='Revenue'),

    html.Div(["Start Date: ",
              dcc.Input(id='start_date', value='2021-02-15', type='text')],
             style={"width": "20%", "float": "left"},
             ),

    html.Div(["End Date: ",
              dcc.Input(id='end_date', value='2021-02-16', type='text')],
             style={"width": "20%", "float": "left"},
             ),

    html.Button('Show Revenue', id='update_revenue'),

    html.Div(
        dcc.Graph(
            id='ios_revenue',
        ),
        style={"width": "50%", "float": "left"},
    ),

    html.Div(
        dcc.Graph(
            id='android_revenue',
        ),
        style={"width": "50%", "float": "left"},
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

    episodes, ep_info, data = preprocess_story_data(story_id)
    data.to_csv('data.csv')
    fig = visualize_story(episodes, ep_info, data)
    return fig


@app.callback(
    [
        Output('ios_revenue', 'figure'),
        Output('android_revenue', 'figure')
    ],
    [
        Input('start_date', 'value'),
        Input('end_date', 'value'),
        Input('update_revenue', 'n_clicks')
    ]
)
def update_multiple_graph(start_date_text, end_date_text, value):
    # if len(start_date_text) < 10 or len(end_date_text) < 10:
    if value is None:
        raise PreventUpdate

    start_date = datetime.datetime.strptime(start_date_text, '%Y-%m-%d').date()
    end_date = datetime.datetime.strptime(end_date_text, '%Y-%m-%d').date()

    _, story_ids = get_story_ids_by_date(start_date, end_date)
    data = get_revenue_per_platform(story_ids)
    ios_fig, and_fig = vis_revenue_per_platfrom(data)
    return ios_fig, and_fig


if __name__ == '__main__':
    app.run_server(debug=True)
