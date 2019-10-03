import numpy as np
import scipy.sparse as sps
from sklearn.feature_extraction.text import TfidfTransformer


class ICM(object):

    def __init__(self, df, col="artist"):

        self.trackList = np.array(df.iloc[:, 0])
        artistList = np.array(df.iloc[:, 1])
        albumList = np.array(df.iloc[:, 2])

        interactionList = np.array([1] * len(self.trackList))
        if col == "artist":
            self.icm_coord = sps.coo_matrix((interactionList, (self.trackList, artistList)), dtype=float)
        elif col == "album":
            self.icm_coord = sps.coo_matrix((interactionList, (self.trackList, albumList)), dtype=float)
        else:
            raise ValueError("not a valid column name")

        #self.icm_coord = TfidfTransformer().fit_transform(self.icm_coord)


    def extractTracksWithAttribute(self, attrNum):
        # columnNum = track_id
        icm_csc = self.getCSC()
        column_start = icm_csc.indptr[attrNum]
        column_end = icm_csc.indptr[attrNum + 1]
        column_data = icm_csc.indices[column_start:column_end]
        return column_data

    def extractAttributesFromTrack(self, trackNum):
        # rowNum = playlist_id
        icm_csr = self.getCSR()
        row_start = icm_csr.indptr[trackNum]
        row_end = icm_csr.indptr[trackNum + 1]
        row_data = icm_csr.indices[row_start:row_end]
        return row_data

    def getNumTracks(self):
        return len(np.unique(self.trackList))

    def getCSC(self):
        return self.icm_coord.tocsc()

    def getCSR(self):
        return self.icm_coord.tocsr()

    def getCoord(self):
        return  self.icm_coord.tocoo()