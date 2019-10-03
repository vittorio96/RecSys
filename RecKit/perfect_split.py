import pandas as pd
import numpy as np

def perfect_split(df, targets, test_size=0.2, threshold=0, method='l20p', alsoinTargets=True):
    """
    Splits the dataset into training and test set.
    Builds the URM train csr matrix and the test dataframe in a
    submission-like structure.
    When default returns the dataset minus the 20% of the
    tracks not in the
    :param threshold: returns train and test of all users whose
                        len is GEQ (>=) of the threshold
    :param method:  '20p' - 20% out of each testable
                    'l20p' - last 20% out of each testable
                    '10' - 10 songs out of each testable
                    'l10' - last 10 songs out of each testable
    """



    # Load the original data set and group by playlist

    # URM_df_seq = self.b.get_train_sequential()

    # Group by playlist_id
    grouped = df.groupby('playlist_id', as_index=True).apply(
        lambda x: list(x['track_id']))
    grouped.sort_index(inplace=True)

    target_playlists = targets

    # Set num_playlist_to_test

    # self.num_playlists_to_test = int(self.b.get_URM().shape[0])
    # self.num_playlists_to_test = int(len(grouped.index) * 0.2)
    # Find indices of playlists to test and set target_playlists

    testable_idx = grouped[[len(x) >= threshold for x in grouped]].index.values.tolist()

    if alsoinTargets:
        testable = testable_idx
    else:
        testable = [value for value in testable_idx if value not in target_playlists]

    num_playlists_to_test = len(testable)
    test_idx = np.random.choice(testable_idx, num_playlists_to_test, replace=False)
    test_idx.sort()

    # Extract the test set portion of the data set

    test_mask = grouped[test_idx]
    test_mask.sort_index(inplace=True)

    # Iterate over the test set to randomly remove 1 tracks from each playlist

    test_df_list = []
    i = 0
    for t in test_mask:

        if method == '20p':
            """ 20% out of each testable"""
            how_many = int(len(t) * test_size)
            t_tracks_to_test = np.random.choice(t, how_many, replace=False)

        elif method == 'l20p':
            """ last 20% out of each testable (assumes sequential)"""
            how_many = int(len(t) * test_size)
            t_tracks_to_test = t[-how_many:]

        elif method == '10':
            """ 10 out of each testable"""
            t_tracks_to_test = np.random.choice(t, 10, replace=False)

        elif method == 'l10':
            """ Last 10 songs out of each testable (assumes sequential)"""
            t_tracks_to_test = t[-10:]


        else:
            raise ValueError("Not a valid method")

        test_df_list.append([test_idx[i], t_tracks_to_test])
        for tt in t_tracks_to_test:
            t.remove(tt)
        i += 1

    # Build test_df and URM_train

    test_df = pd.DataFrame(test_df_list, columns=['playlist_id', 'track_ids'])

    playlist = []
    tracks = []
    for index, row in test_df.iterrows():
        for elem in row[1]:
            playlist.append(row[0])
            tracks.append(elem)

    testCSV = pd.DataFrame({'playlist_id': playlist, 'track_id': tracks})

    df_all = df.merge(testCSV.drop_duplicates(), on=['playlist_id', 'track_id'],
                                   how='left', indicator=True)

    trainCSV = df_all[df_all['_merge'] == 'left_only'].drop(columns=['_merge'])

    return (trainCSV, testCSV)