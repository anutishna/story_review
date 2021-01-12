from db_connection import db_query_dataframe, read_query, update_query

DB_SETTINGS = 'host=localhost dbname=addicted \
    user=addicted_r password=addicted_r port=3456'


def get_df_from_db(query_path, story):
    """Функция для выполнения SQL-запроса, расположенного по указанному адресу."""
    query = read_query(query_path)
    query = update_query(query, params={'story_id' : story})
    return db_query_dataframe(DB_SETTINGS, query)


def get_story_ids_by_date(start_date, end_date):
    """Функция для выявления списка идентификаторов историй, опубликованных за определенный период."""
    query = read_query('sqls/get_story_ids_by_date.sql')
    query = update_query(query, params={'start_date': start_date, 'end_date': end_date})
    df = db_query_dataframe(DB_SETTINGS, query)
    return df, df['story_id']


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