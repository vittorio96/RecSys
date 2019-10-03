from scipy.sparse.linalg import svds
import numpy as np

class SVDRecommender:

    def __init__(self, matr, nf=385, mode="urm", icm_urm=None):
        self.mode = mode

        print("Starting SVD")
        if mode == "urm":
            self.urm = matr
        elif mode == "icm":
            self.icm = matr
            self.urm = icm_urm
        else: raise ValueError("choice not supported")

        self.U, sigma, self.Vt = svds(matr.getCoord(), k=nf)
        self.sigma = np.diag(sigma)
        print(type(self.U), type(self.sigma), type(self.Vt))
        print(self.U, self.sigma, self.Vt)
        self.model = (self.U.dot(self.sigma)).dot(self.Vt)
        print("SVD PredR of type{} finished".format(type(self.model)))



    def s_recommend(self, u, nRec=10):
        if self.mode == "urm":
            row_data = self.model[u]
        elif self.mode =="icm":
            print(type(self.model))
            row_data = np.dot(self.urm.extractPlaylistsFromTrack(u, fullBinVec=True), self.model[:][u])
        else:
            print("Mode not supported")

        # sort by rating
        ranking = np.argsort(-row_data)

        # filter seen
        recommended_items = self._filter_seen(u, ranking)

        return recommended_items[0:nRec]

    def m_recommend(self, target_ids, nRec=10):
        results = []
        for tid in target_ids:
            results.append(self.s_recommend(tid, nRec))
        return results

    def _filter_seen(self, user_id, ranking):
        seen = self.urm.extractTracksFromPlaylist(user_id)
        unseen_mask = np.in1d(ranking, seen, assume_unique=True, invert=True)
        return ranking[unseen_mask]

    def get_pred_row(self, user):
        return self.model[user]