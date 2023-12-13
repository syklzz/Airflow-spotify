import pandas as pd

LOW = 0
MIDDLE = 1
HIGH = 2
COLUMN = 'loudness'


def calculate_loudness_ranges_etl():
    df = calculate_loudness_ranges()
    df.to_csv('../data/ranges.csv', index=False)


def calculate_loudness_ranges():
    df = pd.read_csv('../data/spotify_songs.csv')
    df = df.sort_values(by=COLUMN)
    x0 = df[COLUMN].min()
    x3 = df[COLUMN].max()
    x1 = x0 + (x3 - x0) / 3
    x2 = x0 + 2 * ((x3 - x0) / 3)
    return pd.DataFrame({'x0': [x0], 'x1': [x1], 'x2': [x2], 'x3': [x3]})


def count_genre_within_loudness_range_etl(loudness_range):
    loudness_ranges = pd.read_csv('../data/ranges.csv')
    df = count_genre_within_loudness_range(loudness_range, loudness_ranges)
    if loudness_range == LOW:
        df.to_csv('../data/low_group.csv', index=False)
    elif loudness_range == MIDDLE:
        df.to_csv('../data/middle_group.csv', index=False)
    else:
        df.to_csv('../data/high_group.csv', index=False)


def count_genre_within_loudness_range(loudness_range, loudness_ranges):
    df = pd.read_csv('../data/spotify_songs.csv')
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


def compare_loudness_range_for_genres_etl():
    low_df = pd.read_csv('../data/low_group.csv')
    middle_df = pd.read_csv('../data/middle_group.csv')
    high_df = pd.read_csv('../data/high_group.csv')
    compare_loudness_range_for_genres(low_df, middle_df, high_df)


def compare_loudness_range_for_genres(low_df, middle_df, high_df):
    df = pd.merge(low_df, middle_df, on='playlist_genre', how='outer').fillna(0)
    df = pd.merge(df, high_df, on='playlist_genre', how='outer').fillna(0)
    df.to_csv('../data/merged.csv', index=False)


if __name__ == "__main__":

    # one dag flow test
    ranges = calculate_loudness_ranges()
    low_group = count_genre_within_loudness_range(LOW, ranges)
    middle_group = count_genre_within_loudness_range(MIDDLE, ranges)
    high_group = count_genre_within_loudness_range(HIGH, ranges)
    compare_loudness_range_for_genres(low_group, middle_group, high_group)


if __name__ == "__main__":

    # multiple dag flow test
    calculate_loudness_ranges_etl()
    count_genre_within_loudness_range_etl(LOW)
    count_genre_within_loudness_range_etl(MIDDLE)
    count_genre_within_loudness_range_etl(HIGH)
    compare_loudness_range_for_genres_etl()
