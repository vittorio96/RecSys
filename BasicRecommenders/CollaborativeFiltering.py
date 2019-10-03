import numpy as np

from RecKit.Cosine_Similarity import Cosine_Similarity
from sklearn.preprocessing import normalize


class CollaborativeFiltering(object):

    def fit(self, urm, k=125, h=10, mode='item'):
        self.urm = urm
        self.mode = mode
        print("Building similarity matrix")
        cosineSimilarity = Cosine_Similarity(urm, TopK=k, shrinkage=h)
        cosineSimilarityMatrix = cosineSimilarity.compute_similarity()
        self.cosineSimilarityMatrix = normalize(cosineSimilarityMatrix, norm='l2', axis=1)
        print("Similarity matrix ready for CF")

        if(mode=="user"):
            self.urm_t = self.urm.getCSR().transpose()

        if (self.mode == "item"):
            self.sparse_pred_urm = urm.getCSR().dot(self.cosineSimilarityMatrix)
            print("Prediction matrix for CF-UB ready")
        else:

            self.sparse_pred_urm = self.cosineSimilarityMatrix.dot(self.urm_t)
            print("Prediction matrix for CF-IB ready")


    def s_recommend(self, u, nRec=10):
        pred_row_sparse = self.get_pred_row(u)
        pred_row = np.array(pred_row_sparse.todense()).squeeze()

        ranking = np.argsort(-pred_row)
        recommended_items = self._filter_seen(u, ranking)

        return recommended_items[0:nRec]

    def get_pred_row(self, u):
        return self.sparse_pred_urm.getrow(u)

    def m_recommend(self, target_ids, nRec=10):
        results = []
        for tid in target_ids:
            results.append(self.s_recommend(tid, nRec))
        return results

    def _filter_seen(self, user_id, ranking):
        if self.mode == "item":
            seen = self.urm.extractTracksFromPlaylist(user_id)
        elif self.mode == "user":
            seen = self.urm.extractPlaylistsFromTrack(user_id)
        unseen_mask = np.in1d(ranking, seen, assume_unique=True, invert=True)
        return ranking[unseen_mask]



