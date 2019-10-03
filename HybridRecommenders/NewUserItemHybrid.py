from operator import itemgetter

from BasicRecommenders.ImprovedCBF import ImprovedCBF
from BasicRecommenders.SVDRecommender import SVDRecommender
from HybridRecommenders.ItemItemHybridRecommender import IIHybridRecommender
from BasicRecommenders.CollaborativeFiltering import CollaborativeFiltering

import numpy as np

class NewUserItemHybrid():

    def __init__(self, urm, urm_t, icm, enableSVD=False, urm_full=None):
        self.urm = urm

        self.enableSVD = enableSVD
        if enableSVD:
            self.svd = SVDRecommender(urm, nf=385)

        # Item based
        self.cbi = CollaborativeFiltering()
        self.cbi.fit(urm, k=125, h=10, mode='item')

        # Item based
        self.cbf = ImprovedCBF()
        self.cbf.fit(urm, k=125, h=10, mode='item')

        # User based
        self.cbu = CollaborativeFiltering()
        self.cbu.fit(urm_t, k=100, h=8, mode='user')


    def fit(self, user_weight=1, item_weight=1, svd_weight =0, method='weight_norm'):
        self.user_weight = user_weight
        self.item_weight = item_weight
        self.svd_weight = svd_weight
        self.method = method

    def s_recommend(self, user, nRec=10, switchTH="15"):

        if self.method == 'weight':
            recommended_items_user = self.cbu.s_recommend(user, nRec=nRec + 3)
            recommended_items_item_item = self.iiHybrid.s_recommend(user, nRec=nRec + 3)
            return self.mixRecommendersRow(recommended_items_user, recommended_items_item_item,
                                           nRec)
        elif self.method == 'weight_norm':
            recommended_items_user = self.cbu.pred_urm[user][:]
            user_norm_factor = recommended_items_user.sum()
            recommended_items_user = recommended_items_user/user_norm_factor

            recommended_items_item_item = self.iiHybrid.pred_urm[user][:]
            item_norm_factor = recommended_items_item_item.sum()
            recommended_items_item_item = recommended_items_item_item / item_norm_factor


            recommended_items_svd=None
            if(self.enableSVD):
                recommended_items_svd = self.svd.model[user]
                svd_norm_factor = recommended_items_svd.sum()
                recommended_items_svd = recommended_items_svd / svd_norm_factor

            return self.predWeightRatingRows(user, nRec, recommended_items_user, recommended_items_item_item, recommended_items_svd)

        elif self.method == 'switch':

            if len(self.urm.extractTracksFromPlaylist(user)) < switchTH:
                # enough recommendations, use user
                return self.cbu.s_recommend(user, nRec=nRec)
            else:
                # not enough recommendations, use item
                return self.iiHybrid.s_recommend(user, nRec=nRec)

        else:
            raise ValueError('Not a valid hybrid method')

    def m_recommend(self, user_ids, nRec=10):
        results = []
        for uid in user_ids:
            results.append(self.s_recommend(uid, nRec))
        return results

    def mixRecommendersRow(self, recommended_items_user, recommended_items_item_item,
                           nRec):

        # assign a score based on position

        # initialize
        result = {}
        for i in range(nRec + 3):
            result[str(recommended_items_user[i])] = 0
            result[str(recommended_items_item_item[i])] = 0

        # weight user based cf items
        for i in range(nRec + 3):
            result[str(recommended_items_user[i])] += (nRec - i) * self.user_weight

        # weight item based cf items
        for j in range(nRec + 3):
            result[str(recommended_items_item_item[j])] += (nRec - j) * self.item_weight

        # sort the dict
        sorted_results = sorted(result.items(), key=itemgetter(1))
        rec_items = [x[0] for x in sorted_results]

        # flip to order by decreasing order
        rec_items = rec_items[::-1]

        # return only the topN recommendations
        return np.array(rec_items[0:nRec]).astype(int)

    def predWeightRatingRows(self, user, nRec, recommended_items_user, recommended_items_item_item, recommended_items_svd=None):
        pred_row = recommended_items_user * self.user_weight + recommended_items_item_item * self.item_weight
        if self.enableSVD:
            pred_row = pred_row + self.svd_weight * recommended_items_svd
        ranking = np.argsort(-pred_row)
        recommended_items = self._filter_seen(user, ranking)

        return recommended_items[0:nRec]

    def _filter_seen(self, user_id, ranking):
        seen = self.urm.extractTracksFromPlaylist(user_id)
        unseen_mask = np.in1d(ranking, seen, assume_unique=True, invert=True)
        return ranking[unseen_mask]

