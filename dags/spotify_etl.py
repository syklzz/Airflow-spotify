import sqlite3
import pandas as pd

LOW = 0
MIDDLE = 1
HIGH = 2
BUCKET_COL = 'loudness'
GROUP_COL = 'track_genre'

# DB_LOCATION = '/Users/aszynali/my_workspace/Airflow-spotify/spotify.db'
# DATA_PATH = '/Users/aszynali/my_workspace/Airflow-spotify/data/spotify_songs.csv'

DB_LOCATION = '/home/ewier/Desktop/adzd_project/spotify.db'
# DATA_PATH = '/home/ewier/Desktop/adzd_project/Airflow-spotify/data/dataset_2mb.csv'
DATA_PATH = '/home/ewier/Desktop/adzd_project/Airflow-spotify/data/dataset_large.csv'  # 1.87Gb


def save_data(df, name):
    conn = sqlite3.connect(DB_LOCATION)
    df.to_sql(name=name, con=conn, if_exists='replace')
    conn.close()


def read_data(name):
    conn = sqlite3.connect(DB_LOCATION)
    df = pd.read_sql_query(f'select * from {name};', conn)
    conn.close()
    return df


def run_read_data_etl():
    df = pd.read_csv(DATA_PATH, usecols=[BUCKET_COL, GROUP_COL])
    conn = sqlite3.connect('spotify.db')
    df.to_sql(name='songs', con=conn, if_exists='replace')
    conn.close()


def run_calculate_bucket_ranges_etl():
    df = calculate_bucket_ranges()
    save_data(df, 'ranges')


def calculate_bucket_ranges():
    df = read_data('songs')
    df = df.sort_values(by=BUCKET_COL)
    x0 = df[BUCKET_COL].min()
    x3 = df[BUCKET_COL].max()
    x1 = x0 + (x3 - x0) / 3
    x2 = x0 + 2 * ((x3 - x0) / 3)
    return pd.DataFrame({'x0': [x0], 'x1': [x1], 'x2': [x2], 'x3': [x3]})


def run_count_within_bucket_range_etl(loudness_range):
    loudness_ranges = read_data('ranges')
    df = count_within_bucket_range(loudness_range, loudness_ranges)
    conn = sqlite3.connect(DB_LOCATION)
    if loudness_range == LOW:
        df.to_sql(name='low', con=conn, if_exists='replace')
    elif loudness_range == MIDDLE:
        df.to_sql(name='middle', con=conn, if_exists='replace')
    else:
        df.to_sql(name='high', con=conn, if_exists='replace')
    conn.close()


def count_within_bucket_range(loudness_range, loudness_ranges):
    df = read_data('songs')
    if loudness_range == LOW:
        min_threshold = loudness_ranges['x0'].values[0]
        max_threshold = loudness_ranges['x1'].values[0]
    elif loudness_range == MIDDLE:
        min_threshold = loudness_ranges['x1'].values[0]
        max_threshold = loudness_ranges['x2'].values[0]
    else:
        min_threshold = loudness_ranges['x2'].values[0]
        max_threshold = loudness_ranges['x3'].values[0]
    df[GROUP_COL] = df[(df[BUCKET_COL] > min_threshold) & (df[BUCKET_COL] < max_threshold)][GROUP_COL]
    return df.groupby(GROUP_COL).size().reset_index(name=f'count_{loudness_range}')


def run_compare_result_for_bucket_ranges_etl():
    low_df = read_data('low')
    middle_df = read_data('middle')
    high_df = read_data('high')
    compare_genres_count_loudness_ranges(low_df, middle_df, high_df)


def run_simple_compare_result_for_bucket_ranges_etl():
    loudness_ranges = calculate_bucket_ranges()
    low_df = count_within_bucket_range(LOW, loudness_ranges)
    middle_df = count_within_bucket_range(MIDDLE, loudness_ranges)
    high_df = count_within_bucket_range(HIGH, loudness_ranges)
    compare_genres_count_loudness_ranges(low_df, middle_df, high_df, 'simple_result')


def compare_genres_count_loudness_ranges(low_df, middle_df, high_df, name='result'):
    df = pd.merge(low_df, middle_df, on=GROUP_COL, how='outer').fillna(0)
    df = pd.merge(df, high_df, on=GROUP_COL, how='outer').fillna(0)
    conn = sqlite3.connect(DB_LOCATION)
    df.to_sql(name=name, con=conn, if_exists='replace')
    conn.close()
