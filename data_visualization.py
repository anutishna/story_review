import plotly.graph_objects as go
import pandas as pd

def get_ea_markers(ep_info_df):
    rows = []
    date = ep_info_df['date'][0]
    ea_count = ep_info_df['ea_count'][0]
    r = {'date': date, 'ea_count': ea_count}
    rows.append(r)

    for _, row in ep_info_df.iterrows():
        if ea_count != row['ea_count']:
            date = row['date']
            ea_count = row['ea_count']
            new_r = {'date': date, 'ea_count': ea_count}
            rows.append(new_r)

    return pd.DataFrame.from_dict(rows)


def ea_markers_trace(ep_info_df, height):
    ea_markers = get_ea_markers(ep_info_df)
    ea_markers['height'] = height

    return go.Scatter(
        x=ea_markers['date'],
        y=ea_markers['height'],
        mode="markers+text",
        name="EA episodes",
        text=ea_markers['ea_count'],
        legendgroup='eps',
        textposition="top center"
    )


def published_eps_trace(episodes_df, height):
    markers = episodes_df[['published_at', 'ep_num']].groupby('published_at').max().reset_index()
    markers['height'] = height

    return go.Scatter(
        x=markers['published_at'],
        y=markers['height'],
        mode="markers+text",
        name="episodes published",
        text=markers['ep_num'],
        legendgroup='eps',
        textposition="bottom center"
    )