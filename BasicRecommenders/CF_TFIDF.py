import numpy as np
import scipy as sc

from scipy import sparse
from sklearn import feature_extraction
from sklearn.preprocessing import normalize
from tqdm import tqdm


class CollaborativeFiltering_TFIDF(object):

    def fit(self, urm, k=125, h=10, mode='item'):
        self.urm = urm
        self.mode = mode

        UCM_tfidf = feature_extraction.text.TfidfTransformer().fit_transform(urm.getCSR().T)

        UCM = UCM_tfidf.T.tocsr()
        UCM_T = UCM.T.tocsr()

        s_matrix_list = []
        for i in tqdm(range(0, UCM.shape[1])):

            s_row = UCM_T[i] * UCM
            r = s_row.data.argsort()[:-k]
            s_row.data[r] = 0

            sparse.csr_matrix.eliminate_zeros(s_row)
            s_matrix_list.append(s_row)

        self.cosineSimilarityMatrix = sc.sparse.vstack(s_matrix_list)
        self.cosineSimilarityMatrix.setdiag(0)

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



