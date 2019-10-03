from BasicRecommenders.ContentBasedFiltering import ContentBasedFiltering
from BasicRecommenders.CollaborativeFiltering import CollaborativeFiltering
import scipy.sparse as sps
import numpy as np

class IIHybridRecommender():

    def __init__(self, urm, icm, icm2):
        self.urm = urm

        # Content based 1
        self.cbf = ContentBasedFiltering(icm, urm, k=25, shrinkage=10)
        self.cbf.fit()
        print("CBF1 finished")

        # Content based 1
        self.cbf2 = ContentBasedFiltering(icm2, urm, k=25, shrinkage=10)
        self.cbf2.fit()
        print("CBF2 finished")

        # Item based
        self.cbi = CollaborativeFiltering()
        self.cbi.fit(urm, k=125, h=10, mode='item')
        print("Item CF finished")


    def fit(self, item_weight, cbf1_weight, cbf2_weight):

        print("Building hybrid model")
        hybrid_similarity = self.mix_similarity_rows(item_weight, cbf1_weight, cbf2_weight)
        print("Weighted similarity finished for Item-Item Hybrid")
        #Computing predictions
        self.sparse_pred_urm = self.urm.getCSR().dot(hybrid_similarity)

        print("Pred R finished for Item-Item Hybrid")


    def mix_similarity_matrices(self, item_weight, cbf1_weight, cbf2_weight):

        hybrid_sim_matrix = item_weight * self.cbi.cosineSimilarityMatrix + cbf1_weight * self.cbf.W_sparse \
                            +cbf2_weight * self.cbf2.W_sparse
        return hybrid_sim_matrix



    def mix_similarity_rows(self, item_weight, cbf1_weight, cbf2_weight):

        items = self.cbi.cosineSimilarityMatrix.shape[0]

        norm_method ="max"
        #initialize hybrid matrix
        cbi_row = self.normalize_row(self.cbi.cosineSimilarityMatrix.getrow(0),norm_method)
        cbf1_row = self.normalize_row(self.cbf.W_sparse.getrow(0), norm_method)
        cbf2_row = self.normalize_row(self.cbf2.W_sparse.getrow(0), norm_method)
        hybrid_row = cbi_row + cbf1_row + cbf2_row

        self.weighted_sim_matrix = hybrid_row
        #fill the entire hybrid matrix

        for item in range(1, items):
            cbi_row = self.cbi.cosineSimilarityMatrix.getrow(item)
            cbf1_row = self.cbf.W_sparse.getrow(item)
            cbf2_row = self.cbf2.W_sparse.getrow(item)

            cbi_row = self.normalize_row(cbi_row, norm_method)
            cbf1_row = self.normalize_row(cbf1_row, norm_method)
            cbf2_row = self.normalize_row(cbf2_row, norm_method)


            """"
            item_popularity = len(self.urm.extractPlaylistsFromTrack(item))
            cbi_extra_weight = 0
            
            if item_popularity < 5:
                cbi_extra_weight = - 0.1
            elif item_popularity > 300:
                cbi_extra_weight = 0.1 + item_popularity/1000"""

            hybrid_row = item_weight * cbi_row + cbf1_weight * cbf1_row + cbf2_weight * cbf2_row
            #0.8 , 0.3 0.2

            self.weighted_sim_matrix = sps.vstack([self.weighted_sim_matrix, hybrid_row], "csr")

        return self.weighted_sim_matrix


    def s_recommend(self, u, nRec=10):

        pred_row_sparse = self.get_pred_row(u)
        pred_row = np.array(pred_row_sparse.todense()).squeeze()

        ranking = np.argsort(-pred_row)
        recommended_items = self._filter_seen(u, ranking)

        return recommended_items[0:nRec]

    def get_pred_row(self, u):
        return self.sparse_pred_urm.getrow(u)

    def m_recommend(self, user_ids, nRec=10):

        results = []
        for uid in user_ids:
            results.append(self.s_recommend(uid, nRec))
        return results

    def _filter_seen(self, user_id, ranking):
        user_profile = self.urm.getCSR()[user_id]
        seen = user_profile.indices
        unseen_mask = np.in1d(ranking, seen, assume_unique=True, invert=True)
        return ranking[unseen_mask]

    def normalize_row(self, similarity_row, method):
        if method == 'max':
            norm_factor = similarity_row.max()
            if norm_factor == 0:
                norm_factor = 1
            return similarity_row / norm_factor

        elif method == 'sum':
            norm_factor = similarity_row.sum()
            if norm_factor == 0:
                norm_factor = 1
            return similarity_row / norm_factor

        elif method == 'none':
            return similarity_row

        else:
            raise ValueError('Not a valid normalization method')