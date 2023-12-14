import pandas as pd

LOW = 0
MIDDLE = 1
HIGH = 2
COLUMN = 'loudness'
# data_path = '../data/spotify_songs.csv'
data_path = 'Airflow-spotify/data/dataset_2mb.csv'


def run_calculate_loudness_ranges_etl():
    df = calculate_loudness_ranges()
    df.to_csv('../data/ranges.csv', index=False)


def calculate_loudness_ranges():
    df = pd.read_csv(data_path)
    df = df.sort_values(by=COLUMN)
    x0 = df[COLUMN].min()
    x3 = df[COLUMN].max()
    x1 = x0 + (x3 - x0) / 3
    x2 = x0 + 2 * ((x3 - x0) / 3)
    return pd.DataFrame({'x0': [x0], 'x1': [x1], 'x2': [x2], 'x3': [x3]})


def run_count_genres_within_loudness_range_etl(loudness_range):
    loudness_ranges = pd.read_csv('../data/ranges.csv')
    df = count_genres_within_loudness_range(loudness_range, loudness_ranges)
    if loudness_range == LOW:
        df.to_csv('../data/low_group.csv', index=False)
    elif loudness_range == MIDDLE:
        df.to_csv('../data/middle_group.csv', index=False)
    else:
        df.to_csv('../data/high_group.csv', index=False)


def count_genres_within_loudness_range(loudness_range, loudness_ranges):
    df = pd.read_csv(data_path)
    if loudness_range == LOW:
        min_threshold = loudness_ranges['x0'].values[0]
        max_threshold = loudness_ranges['x1'].values[0]
    elif loudness_range == MIDDLE:
        min_threshold = loudness_ranges['x1'].values[0]
        max_threshold = loudness_ranges['x2'].values[0]
    else:
        min_threshold = loudness_ranges['x2'].values[0]
        max_threshold = loudness_ranges['x3'].values[0]
    df['playlist_genre'] = df[(df[COLUMN] > min_threshold) & (df[COLUMN] < max_threshold)]['playlist_genre']
    return df.groupby('playlist_genre').size().reset_index(name=f'count_{loudness_range}')


def run_compare_genres_count_for_loudness_ranges_etl():
    low_df = pd.read_csv('../data/low_group.csv')
    middle_df = pd.read_csv('../data/middle_group.csv')
    high_df = pd.read_csv('../data/high_group.csv')
    compare_genres_count_loudness_ranges(low_df, middle_df, high_df)


def run_simple_compare_genres_count_for_loudness_ranges_etl():
    loudness_ranges = calculate_loudness_ranges()
    low_df = count_genres_within_loudness_range(LOW, loudness_ranges)
    middle_df = count_genres_within_loudness_range(MIDDLE, loudness_ranges)
    high_df = count_genres_within_loudness_range(HIGH, loudness_ranges)
    compare_genres_count_loudness_ranges(low_df, middle_df, high_df)


def compare_genres_count_loudness_ranges(low_df, middle_df, high_df):
    df = pd.merge(low_df, middle_df, on='playlist_genre', how='outer').fillna(0)
    df = pd.merge(df, high_df, on='playlist_genre', how='outer').fillna(0)
    df.to_csv('../data/merged.csv', index=False)


def run_end_etl():
    print("Success!")
