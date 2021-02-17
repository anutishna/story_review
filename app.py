import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate
from data_getter import get_story_id_by_title
from data_visualization import visualize_story
from data_processing import preprocess_story_data

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(children=[
    html.H1(children='Story Review'),

    html.Div(
        dcc.Graph(
            id='single_story_review',
        ),
        style={"width": "100%", "height": "100%", "float": "left"},
    ),

    # html.Div(
    #     dcc.Graph(
    #         id='multiple_stories_revenue',
    #     ),
    #     style={"width": "40%", "float": "left"},
    # ),

    html.Div(["Story Title: ",
              dcc.Input(id='story_title', value='Изоляция', type='text')],
             style={"width": "100%", "float": "left"},
             ),

    # html.Div(["Start Date: ",
    #           dcc.Input(id='start_date', value='2020-09-07', type='text')],
    #          style={"width": "20%", "float": "left"},
    #          ),
    #
    # html.Div(["End Date: ",
    #           dcc.Input(id='end_date', value='2020-09-13', type='text')],
    #          style={"width": "20%", "float": "left"},
    #          ),

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
    fig = visualize_story(episodes, ep_info, data)
    return fig


# @app.callback(
#     Output('multiple_stories_revenue', 'figure'),
#     [
#         Input('start_date', 'value'),
#         Input('end_date', 'value')
#     ]
# )
# def update_multiple_graph(start_date_text, end_date_text):
#     if len(start_date_text) < 10 or len(end_date_text) < 10:
#         raise PreventUpdate
#
#     start_date = text_to_date(start_date_text)
#     end_date = text_to_date(end_date_text)
#
#     fig_rev, _ = visualize_stories_by_dates(start_date, end_date)
#     return fig_rev


if __name__ == '__main__':
    app.run_server(debug=True)
