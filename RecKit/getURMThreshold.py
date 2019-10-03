import numpy as np

def getURMThreshold(urm, percentile):
    n_rows = urm.getCSR().shape[0]
    num_tracks_per_playlist = [None] * n_rows
    for i in range(n_rows):
        num_tracks_per_playlist[i] = len(urm.extractTracksFromPlaylist(i))
    return int(np.percentile(num_tracks_per_playlist, percentile))