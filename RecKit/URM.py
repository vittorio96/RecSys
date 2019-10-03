import numpy as np
import scipy.sparse as sps
from sklearn.feature_extraction.text import TfidfTransformer


class URM(object):

    #def __init__(self, df):
    def __init__(self, df, n_users=50446, n_items=20635, transposed=False):
        self.playList = np.array(df.iloc[:, 0].tolist())
        self.trackList = np.array(df.iloc[:, 1].tolist())

        print(np.max(self.playList), np.max(self.trackList))
        self.interactionList = np.array([1] * len(self.trackList))
        self.nInteractions = len(self.interactionList)

        if transposed:
            self.urm_coord = sps.coo_matrix((self.interactionList, (self.playList, self.trackList)),
                                            shape=(n_items, n_users), dtype=float)
        else:
            self.urm_coord = sps.coo_matrix((self.interactionList, (self.playList, self.trackList)),
                                            shape=(n_users, n_items), dtype=float)

        self.urm_csc = self.urm_coord.tocsc()
        self.urm_csr = self.urm_coord.tocsr()



    def getNumPlaylists(self):
        return len(np.unique(self.playList))

    def getNumTracks(self):
        return len(np.unique(self.trackList))

    def getPlaylists(self):
        return np.unique(self.playList)

    def getTracks(self):
        return np.unique(self.trackList)

    def getCSC(self):
        return self.urm_csc

    def getCSR(self):
        return self.urm_csr

    def getCoord(self):
        return self.urm_coord

    def setCSC(self, urm_csc):
        self.urm_csc = urm_csc

    def setCSR(self, urm_csr):
        self.urm_csr = urm_csr

    def setCoord(self, urm_csr):
        self.urm_csr = urm_csr

    def extractPlaylistsFromTrack(self, columnNum, fullBinVec=False):
        # columnNum = track_id
        column_start = self.urm_csc.indptr[columnNum]
        column_end = self.urm_csc.indptr[columnNum + 1]
        column_data = self.urm_csc.indices[column_start:column_end]
        if fullBinVec:
            return self.urm_csc[columnNum].todense().A[0]  # I give back the result with 0 and 1
        else:
            return column_data

    def extractTracksFromPlaylist(self, rowNum, fullBinVec=False):
        # rowNum = playlist_id
        row_start = self.urm_csr.indptr[rowNum]
        row_end = self.urm_csr.indptr[rowNum + 1]
        row_data = self.urm_csr.indices[row_start:row_end]
        if fullBinVec:
            return self.urm_csc[rowNum].todense().A[0]
        else:
            return row_data

