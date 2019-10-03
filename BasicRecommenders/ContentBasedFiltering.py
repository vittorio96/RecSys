from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.preprocessing import normalize

from RecKit.Cosine import Cosine
import numpy as np
import scipy.sparse as sps

class ContentBasedFiltering(object):

    def __init__(self, icm, urm, k=125, shrinkage=10, icm2=None):
        self.icm = icm
        self.urm = urm
        self.k = k
        self.shrinkage = shrinkage
        self.distance = Cosine(shrinkage=shrinkage)

    def fit(self):

        item_weights = self.distance.compute(self.icm)
        item_weights = item_weights.tocsr()  # nearly 10 times faster

        # for each column, keep only the top-k scored items
        # THIS IS THE SLOW PART, FIND A BETTER SOLUTION
        values, rows, cols = [], [], []

        nitems = self.icm.getNumTracks()

        for i in range(nitems):
            if (i % 10000 == 0):
                print("Item %d of %d" % (i, nitems))

            this_item_weights = item_weights[i, :].toarray()[0]
            top_k_idx = np.argsort(this_item_weights)[-self.k:]

            values.extend(this_item_weights[top_k_idx])
            rows.extend(np.arange(nitems)[top_k_idx])
            cols.extend(np.ones(self.k) * i)

        W_sparse = sps.csc_matrix((values, (rows, cols)), shape=(nitems, nitems), dtype=np.float32)
        self.W_sparse = normalize(W_sparse, norm='l2', axis=1)
        #self.sparse_pred_urm = self.ucm.dot(self.W_sparse)
        self.sparse_pred_urm = self.urm.getCSR().dot(self.W_sparse)
        print("Prediction matrix of CBF ready")

    def get_pred_row(self, u):
        return self.sparse_pred_urm.getrow(u)

    def s_recommend(self, u, nRec=10):

        pred_row_sparse = self.get_pred_row(u)
        pred_row = np.array(pred_row_sparse.todense()).squeeze()
        ranking = np.argsort(-pred_row)
        recommended_items = self._filter_seen(u, ranking)

        return recommended_items[0:nRec]

    def m_recommend(self, target_ids, nRec=10):
        results = []
        for tid in target_ids:
            results.append(self.s_recommend(tid, nRec))
        return results

    def _filter_seen(self, user_id, ranking):
        user_profile = self.urm.getCSR()[user_id]
        #user_profile = self.ucm[user_id]
        seen = user_profile.indices
        unseen_mask = np.in1d(ranking, seen, assume_unique=True, invert=True)
        return ranking[unseen_mask]