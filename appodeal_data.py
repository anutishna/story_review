import requests
import pandas as pd

API_KEY = '4803e52dadcdf1c582443405eb9eec6b'
USER_ID = '82813'
APP_KEY = '1bf168de5fb48e65d17dc3eb7b1455f456f2f6f13ab4c107'


def get_revenue_by_dates(start_date, end_date, key=API_KEY, user=USER_ID, app=APP_KEY):
    request_params = {
        "api_key": key,
        "user_id": user,
        "date_from": start_date,
        "date_to": end_date,
        "detalisation[]": ['app', 'include_shared_apps']
    }

    resp = requests.get('https://api-services.appodeal.com/api/v2/stats_api', params=request_params)
    task_id = resp.json()['task_id']

    check_params = {
        "api_key": API_KEY,
        "user_id": USER_ID,
        "task_id": task_id
    }

    task_status = 0
    while task_status != '1':
        check = requests.get('https://api-services.appodeal.com/api/v2/check_status', params=check_params)
        task_status = check.json()['task_status']

    info = requests.get('https://api-services.appodeal.com/api/v2/output_result', params=check_params)
    df = pd.DataFrame(info.json()['data'])
    df_abyss = df[df.app_key == app].reset_index()

    return df_abyss, df_abyss.revenue[0]


df, rev = get_revenue_by_dates('2020-12-01', '2020-12-25')
print('Revenue >>', rev)
print('Impressions >>', df.impressions[0])
