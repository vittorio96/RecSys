import pandas as pd

import numpy as np

from RecKit.URM import URM


def getURMThreshold(urm, percentile):
    """
        Gets the threshold (number of tracks) that separates the that separates the lower n % of the
        population and the 100-n%
    """
    n_rows = urm.getCSR().shape[0]
    num_tracks_per_playlist = [None] * n_rows
    for i in range(n_rows):
        num_tracks_per_playlist[i] = len(urm.extractTracksFromPlaylist(i))
    return int(np.percentile(num_tracks_per_playlist, percentile))


def population_train_split(csv, method=('threshold', [14, 35])):
    """
    :param csv: a csv file with playlist_id, track_id
    :param method: cqn be percentile or threshold

    :return: a tuple (train_group1, train_group2)
    """
    urm = URM(csv)
    if method[0] == 'threshold':
        group_1_2_TH = method[1][0]
        group_2_3_TH = method[1][1]

    elif method[0] == 'percentile':
        group_1_2_TH = getURMThreshold(urm, method[1][0])
        group_2_3_TH = getURMThreshold(urm, method[1][1])
    else:
        raise ValueError("not a valid split method")

    unique_playlists = np.unique(np.array(csv.iloc[:, 0].tolist()))
    print("Thresholds: {}, {}".format(group_1_2_TH, group_2_3_TH))

    playlists_group1 = [i for i in unique_playlists if len(urm.extractTracksFromPlaylist(i)) <= group_1_2_TH]
    playlists_group2 = [i for i in unique_playlists
                        if group_1_2_TH < len(urm.extractTracksFromPlaylist(i)) <= group_2_3_TH]
    playlists_group3 = [i for i in unique_playlists if len(urm.extractTracksFromPlaylist(i)) > group_2_3_TH]

    train_group1 = csv.loc[csv['playlist_id'].isin(playlists_group1)]
    train_group2 = csv.loc[csv['playlist_id'].isin(playlists_group2)]
    train_group3 = csv.loc[csv['playlist_id'].isin(playlists_group3)]

    return (train_group1, train_group2, train_group3)

def population_split(csv, method=('threshold', [7])):
    """
    :param csv: a csv file with playlist_id, track_id
    :param percentile: the percentile that separates the lower n % of the population and the 100-n%
    of the population, this is used instead of a fixed threshold to cope with dynamic playlist length
    (what we mean by "few" playlists depends on the dataset, is not a hardcoded number)

    :return: a tuple (playlists_group1, playlists_group2)
    """
    urm = URM(csv)
    if method[0] == 'threshold':
        group_1_2_TH = method[1][0]
        group_2_3_TH = method[1][1]

    elif method[0] == 'percentile':
        group_1_2_TH = getURMThreshold(urm, method[1][0])
        group_2_3_TH = getURMThreshold(urm, method[1][1])
    else:
        raise ValueError("not a valid split method")

    unique_playlists = np.unique(np.array(csv.iloc[:, 0].tolist()))
    print("Thresholds: {}, {}".format(group_1_2_TH, group_2_3_TH))

    playlists_group1 = [i for i in unique_playlists if len(urm.extractTracksFromPlaylist(i)) <= group_1_2_TH]
    playlists_group2 = [i for i in unique_playlists
                        if group_1_2_TH < len(urm.extractTracksFromPlaylist(i)) <= group_2_3_TH]
    playlists_group3 = [i for i in unique_playlists if len(urm.extractTracksFromPlaylist(i)) > group_2_3_TH]

    return (playlists_group1, playlists_group2, playlists_group3)

def population_target_split(targetList, urm, method):
    """
    :param urm: A CSR sparse User rating matrix
    :param method:
    'percentile': the percentile that separates the lower n % of the population and the 100-n%
    of the population, this is used instead of a fixed threshold to cope with dynamic playlist length
    (what we mean by "few" playlists depends on the dataset, is not a hardcoded number)

    :return: a tuple (playlists_group1, playlists_group2)
    """
    playlists_group1, playlists_group2 = population_train_split(urm, method)
    targets_group1 = np.intersect1d(targetList, playlists_group1)
    targets_group2 = np.intersect1d(targetList, playlists_group2)

    return (targets_group1, targets_group2)