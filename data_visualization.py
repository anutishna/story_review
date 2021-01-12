import plotly.graph_objects as go
import pandas as pd
from plotly.subplots import make_subplots


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


def visualize_story(episodes, ep_info, zero_eps_read, max_eps_read, progress_rev):
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(go.Scatter(
        x=progress_rev['date'],
        y=progress_rev['sum_ea_revenue'],
        name='EA revenue',
        legendgroup='revenue',
        stackgroup='one', mode='none'
    ), secondary_y=True)

    fig.add_trace(go.Scatter(
        x=progress_rev['date'],
        y=progress_rev['sum_ios_subs_revenue'],
        name='iOS subs revenue',
        legendgroup='revenue',
        stackgroup='one', mode='none'
    ), secondary_y=True)

    fig.add_trace(go.Scatter(
        x=progress_rev['date'],
        y=progress_rev['sum_android_subs_revenue'],
        name='Android subs revenue',
        legendgroup='revenue',
        stackgroup='one', mode='none'
    ), secondary_y=True)

    fig.add_trace(go.Scatter(
        x=progress_rev['date'],
        y=progress_rev['sum_ads_revenue'],
        name='Ads revenue',
        legendgroup='revenue',
        stackgroup='one', mode='none'
    ), secondary_y=True)

    fig.add_trace(go.Scatter(
        x=progress_rev['date'],
        y=progress_rev['sum_users_stop_reading'],
        name="users stopped reading",
        mode='lines',
        legendgroup='read'
    ), secondary_y=False)

    fig.add_trace(go.Scatter(
        x=zero_eps_read['date'],
        y=zero_eps_read['sum_users_read'],
        name="users stopped at 0 ep",
        mode='lines',
        legendgroup='read'
    ), secondary_y=False)

    fig.add_trace(go.Scatter(
        x=max_eps_read['date'],
        y=max_eps_read['sum_users_read'],
        name="users finished story",
        mode='lines',
        legendgroup='read'
    ), secondary_y=False)

    fig.add_trace(published_eps_trace(
        episodes, zero_eps_read['users_read'].min()
    ), secondary_y=False)

    fig.add_trace(ea_markers_trace(
        ep_info, progress_rev['sum_users_stop_reading'].max()
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


def add_bar_trace_to_fig(p_fig, x, y, name, secondary_y=False):
    p_fig.add_trace(go.Bar(
        x=x,
        y=y,
        name=name
    ), secondary_y=secondary_y)


def add_scatter_trace_to_fig(p_fig, x, y, name, secondary_y=True,
                             mode='markers+text', size=10, marker_line_width=2,
                             marker_symbol="cross-thin", marker_line_color="darkred",
                             textposition='middle right'):
    p_fig.add_trace(go.Scatter(
        x=x,
        y=y,
        name=name,
        mode=mode,
        marker=dict(size=size),
        marker_line_width=marker_line_width,
        marker_symbol=marker_symbol,
        marker_line_color=marker_line_color,
        text=[round(num, 2) for num in y],
        textposition=textposition,
    ), secondary_y=secondary_y)


def vis_revenue_per_platfrom(ios_data, and_data):
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    add_bar_trace_to_fig(fig, ios_data.title, ios_data.subscribtions, 'Revenue')
    add_scatter_trace_to_fig(fig, ios_data.title, ios_data.rev_per_user, 'Revenue per User')
    fig.update_layout(title='iOS Revenue', barmode='stack', xaxis={'categoryorder': 'total descending'})

    fig_2 = make_subplots(specs=[[{"secondary_y": True}]])
    add_bar_trace_to_fig(fig_2, and_data.title, and_data.subscribtions, 'Subs Revenue')
    add_bar_trace_to_fig(fig_2, and_data.title, and_data.ads, 'Ads Revenue')
    add_bar_trace_to_fig(fig_2, and_data.title, and_data.early_accesses, 'EA Revenue')
    add_scatter_trace_to_fig(fig_2, and_data.title, and_data.rev_per_user_no_ea, 'Revenue per User (no EA)')
    add_scatter_trace_to_fig(fig_2, and_data.title, and_data.rev_per_user_with_ea, 'Revenue per User (with EA)',
                             marker_symbol='x-thin', marker_line_color='darkblue', mode='markers')
    fig_2.update_layout(title='Android Revenue', barmode='stack', xaxis={'categoryorder': 'total descending'})
    return fig, fig_2
