def append_dataset(orig_path, result_path, N):
    data = ""
    with open(orig_path, "r") as f:
        data = f.read()

    with open(result_path, "a") as f:
        f.write(data)

    with open(orig_path, "r") as f:
        next(f)
        data = f.read()

    for _ in range(N):
        with open(result_path, "a") as f:
            f.write(data)


if __name__ == "__main__":
    path1 = "spotify_songs.csv"
    path2 = "spotify_songs_large.csv"
    append_dataset(path1, path2, N=100)
