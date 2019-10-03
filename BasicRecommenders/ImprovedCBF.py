from sklearn.preprocessing import normalize

from RecKit.Cosine import Cosine
import numpy as np
import scipy.sparse as sps





class ImprovedCBF(object):

    def __init__(self, icm, icm2, urm, k=125, shrinkage=10, ar_weight=1, al_weight=1):
        self.icm2 = icm2
        self.icm1 = icm
        self.urm = urm
        self.k = k
        self.shrinkage = shrinkage
        self.distance = Cosine()
        self.setWeights(ar_weight, al_weight)

    def fit(self, norm=None):
        self.fit1()
        self.fit2()
        self.build_compound_similarity(norm)

    def fit1(self): # ARTIST

        item_weights = self.distance.compute(self.icm1)
        item_weights = item_weights.tocsr()  # nearly 10 times faster

        # for each column, keep only the top-k scored items
        # THIS IS THE SLOW PART, FIND A BETTER SOLUTION
        values, rows, cols = [], [], []

        nitems = self.icm1.getNumTracks()

        for i in range(nitems):
            if (i % 10000 == 0):
                print("Item %d of %d" % (i, nitems))

            this_item_weights = item_weights[i, :].toarray()[0]
            top_k_idx = np.argsort(this_item_weights)[-self.k:]

            values.extend(this_item_weights[top_k_idx])
            rows.extend(np.arange(nitems)[top_k_idx])
            cols.extend(np.ones(self.k) * i)

        self.W_sparse1 = sps.csc_matrix((values, (rows, cols)), shape=(nitems, nitems), dtype=np.float32)
        self.sparse_pred_urm1 = self.urm.getCSR().dot(self.W_sparse1)


    def fit2(self): # ALBUM
        item_weights = self.distance.compute(self.icm2)
        item_weights = item_weights.tocsr()  # nearly 10 times faster

        # for each column, keep only the top-k scored items
        # THIS IS THE SLOW PART, FIND A BETTER SOLUTION
        values, rows, cols = [], [], []

        nitems = self.icm2.getNumTracks()

        for i in range(nitems):
            if (i % 10000 == 0):
                print("Item %d of %d" % (i, nitems))

            this_item_weights = item_weights[i, :].toarray()[0]
            top_k_idx = np.argsort(this_item_weights)[-self.k:]

            values.extend(this_item_weights[top_k_idx])
            rows.extend(np.arange(nitems)[top_k_idx])
            cols.extend(np.ones(self.k) * i)

        self.W_sparse2 = sps.csc_matrix((values, (rows, cols)), shape=(nitems, nitems), dtype=np.float32)
        self.sparse_pred_urm2 = self.urm.getCSR().dot(self.W_sparse2)

    def setWeights(self, ar_weight=1, al_weight=1):
        self.ar_w = ar_weight
        self.al_w = al_weight

    def get_pred_row(self, u):
        return self.get_ar_row(u) * self.ar_w + self.get_al_row(u) * self.al_w

    def s_recommend(self, u, nRec=10):
        pred_row = self.get_pred_row(u)
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
        seen = user_profile.indices
        unseen_mask = np.in1d(ranking, seen, assume_unique=True, invert=True)
        return ranking[unseen_mask]

    def normalizerow(self, row):
        norm_factor = row.max()
        return row / norm_factor

    def get_ar_row(self,u):
        return self.sparse_pred_urm1.getrow(u)

    def get_al_row(self, u):
        return self.sparse_pred_urm2.getrow(u)

    def build_compound_similarity(self, norm):
        self.W_sparse = self.W_sparse1 * self.ar_w + self.W_sparse2 * self.al_w

        if norm != None:
            "Normalizing"
            self.W_sparse = normalize(self.W_sparse, norm=norm, axis=1)
