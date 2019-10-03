from operator import itemgetter

from BasicRecommenders.ContentBasedFiltering import ContentBasedFiltering
from HybridRecommenders.ItemItemHybridRecommender import IIHybridRecommender
from BasicRecommenders.CollaborativeFiltering import CollaborativeFiltering

import numpy as np

class UserItemHybridRecommender_v3():

    def __init__(self, urm, urm_t, icm, icm2, weights_dict):

        self.urm = urm
        #setting ite-item hybrid weights
        self.item_weight = weights_dict.get('item_weight', 0)
        self.cbf_weight = weights_dict.get('cbf_weight', 0)
        self.cbf2_weight = weights_dict.get('cbf2_weight', 0)

        # User based
        print("starting USER CF")
        self.cbu = CollaborativeFiltering()
        self.cbu.fit(urm_t, k=100, h=8, mode='user')
        print("USER CF finished")

        # Item-item hybrid recommender
        print("starting ITEM-ITEM HYBRID")
        self.iih = IIHybridRecommender(urm, icm, icm2)
        self.iih.fit(self.item_weight, self.cbf_weight, self.cbf2_weight)
        print("ITEM-ITEM HYBRID finished")


    def fit(self, enable_dict, weights_dict, method='weight_norm'):

        self.setEnables(enable_dict)

        self.user_weight = weights_dict.get('user_weight', 0)
        self.svd_weight = weights_dict.get('svd_weight', 0)

        self.method = method


    def s_recommend(self, user, nRec=10, switchTH="15"):


        if self.method == 'weight_norm':

            norm_method = 'max'

            recommended_items_user = self.normalize_row(self.cbu.get_pred_row(user), method=norm_method)
            recommended_items_iiHybrid = self.normalize_row(self.iih.get_pred_row(user), method=norm_method)

            return self.predWeightRatingRows(user, nRec, recommended_items_user, recommended_items_iiHybrid)

        elif self.method == 'switch':

            if len(self.urm.extractTracksFromPlaylist(user)) < switchTH:
                # enough recommendations, use user
                return self.cbu.s_recommend(user, nRec=nRec)
            else:
                # not enough recommendations, use item
                return self.cbi.s_recommend(user, nRec=nRec)

        else:
            raise ValueError('Not a valid hybrid method')


    def m_recommend(self, user_ids, nRec=10):
        results = []
        for uid in user_ids:
            results.append(self.s_recommend(uid, nRec))
        return results


    def predWeightRatingRows(self, user, nRec, recommended_items_user, recommended_items_iiHybrid):

        """
        playlist_tracks = self.urm.extractTracksFromPlaylist(user)
        num_tracks = playlist_tracks.size
        extra_weight = num_tracks / 1000

        if(num_tracks > 8):
            extra_weight += 0.03
            if(num_tracks > 15):
                extra_weight += 0.03
                if (num_tracks > 20):
                    extra_weight += 0.03
                    if (num_tracks > 33):
                        extra_weight += 0.04"""


        pred_row_sparse = recommended_items_user * self.user_weight + recommended_items_iiHybrid * self.item_weight


        # needs to be before svd because svd output is dense
        pred_row = np.array(pred_row_sparse.todense()).squeeze()

        ranking = np.argsort(-pred_row)
        recommended_items = self._filter_seen(user, ranking)

        return recommended_items[0:nRec]


    def _filter_seen(self, user_id, ranking):
        seen = self.urm.extractTracksFromPlaylist(user_id)
        unseen_mask = np.in1d(ranking, seen, assume_unique=True, invert=True)
        return ranking[unseen_mask]



    def setEnables(self, enable_dict):
        self.enableSVD = enable_dict.get('enableSVD')
        self.enableSLIM = enable_dict.get('enableSLIM')
        self.enableCBF2 = enable_dict.get('enableCBF2')

    def normalize_row(self, recommended_items , method):
        if method == 'max':
            norm_factor = recommended_items.max()
            if norm_factor == 0: norm_factor = 1
            return recommended_items / norm_factor

        elif method == 'sum':
            norm_factor = recommended_items.sum()
            if norm_factor == 0: norm_factor = 1
            return recommended_items / norm_factor

        else:
            raise ValueError('Not a valid normalization method')