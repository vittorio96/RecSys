from operator import itemgetter

from BasicRecommenders.RP3beta import RP3betaRecommender
from BasicRecommenders.CollaborativeFiltering import CollaborativeFiltering
from sklearn.preprocessing import normalize

import numpy as np

from HybridRecommenders.ItemItemHybridRecommender import IIHybridRecommender
from BasicRecommenders.LightFMRecommender import LightFMRecommender
from BasicRecommenders.P3alpha import P3alpha
from MatrixFactorization.PureSVD import PureSVDRecommender
from SLIM_BPR2.Cython.SLIM_BPR_Cython import SLIM_BPR_Cython


class UserItemHybridRecommender():

    def __init__(self, urm, urm_t, icm, icm2, enable_dict, urm_test=None):
        self.urm = urm
        self.setEnables(enable_dict)

        if self.enableRP3B:
            self.rp3b = RP3betaRecommender(urm.getCSR())
            self.rp3b.fit(topK= 100, alpha=0.7, beta=0.3,
                          normalize_similarity= True,
                          implicit=True)


        if self.enableSLIM:
            choice= 2
            logFile = open("SLIM_BPR_Cython.txt", "a")

            self.slim = SLIM_BPR_Cython(urm.getCSR(), recompile_cython=False, positive_threshold=0,
                                   URM_validation=urm_test.getCSR(),final_model_sparse_weights=True,
                                   train_with_sparse_weights=False)

            self.slim.fit(epochs=100, validation_every_n=1, logFile=logFile, batch_size=5, topK=200,
                         sgd_mode="adagrad", learning_rate=0.075)

            self.slim_sim = self.slim.get_similarity()

        if self.enableP3A:
            self.p3a = P3alpha(urm.getCSR())
            self.p3a.fit(topK=80, alpha=1, min_rating=0, implicit=True, normalize_similarity=True)

        # if self.enableCBF2:
        #     print("starting CBF2")
        #     self.cbf2 = ContentBasedFiltering(icm2, urm, k=25, shrinkage=0)
        #     self.cbf2.fit()
        #     print("CBF2 finished")

        if self.enableLFM:
            # LightFM
            print("starting USER CF")
            self.lfm = LightFMRecommender()
            self.lfm.fit(urm, epochs=100)
            print("USER CF finished")

        if self.enableSVD:
            self.svd = PureSVDRecommender(urm.getCSR())
            self.svd.fit(num_factors=225)
            print("USER CF finished")

        # User based
        print("starting USER CF")
        self.cbu = CollaborativeFiltering()
        self.cbu.fit(urm_t, k=100, h=0, mode='user')
        print("USER CF finished")

        self.item_item = IIHybridRecommender(urm, icm, icm2)
        self.item_item.fit(item_weight=0.4, cbf1_weight=0.25, cbf2_weight=0.1)

        # # Item based
        # print("starting ITEM CF")
        # self.cbi = CollaborativeFiltering()
        # self.cbi.fit(urm, k=125, h=0, mode='item')
        # print("ITEM CF finished")
        #
        # # Content based artist
        # print("starting CBF")
        # self.cbf = ContentBasedFiltering(icm, urm, k=25, shrinkage=0)
        # self.cbf.fit()
        # print("CBF finished")



    def fit(self, weights_dict, method='rating_weight', norm='max'):

        self.svd_weight  = weights_dict.get('svd_weight' ,0)
        self.user_weight = weights_dict.get('user_weight',0)
        self.item_weight = weights_dict.get('item_weight',0)
        self.cbf_weight  = weights_dict.get('cbf_weight' ,0)
        self.cbf2_weight = weights_dict.get('cbf2_weight',0)
        self.rp3b_weight = weights_dict.get('rp3b_weight',0)
        self.slim_weight = weights_dict.get('slim_weight',0)
        self.p3a_weight  = weights_dict.get('p3a_weight' ,0)
        self.lfm_weight  = weights_dict.get('lfm_weight' ,0)

        self.method = method
        self.norm = norm

    def s_recommend(self, user, nRec=10, switchTH="15"):

        if self.method == 'item_weight':
            extra = 1

            recommended_items_user = self.cbu.s_recommend(user, nRec+extra)
            recommended_items_item = self.cbi.s_recommend(user, nRec+extra)
            recommended_items_cbf = self.cbf.s_recommend(user, nRec+extra)

            weighting_dict = {'user': (recommended_items_user, self.user_weight),
                              'item': (recommended_items_item, self.item_weight),
                              'cbf' : (recommended_items_cbf , self.cbf_weight)}

            if (self.enableCBF2):
                recommended_items_cbf2 = self.cbf2.s_recommend(user, nRec+extra)
                weighting_dict['cbf2'] = (recommended_items_cbf2, self.cbf2_weight)

            if (self.enableLFM):
                recommended_items_lfm = self.lfm.s_recommend(user, nRec + extra)
                weighting_dict['lfm'] = (recommended_items_lfm, self.lfm_weight)

            if (self.enableSVD):
                recommended_items_svd = self.svd.s_recommend(user, nRec+extra)
                weighting_dict['svd'] = (recommended_items_svd, self.svd_weight)

            if (self.enableSLIM):
                recommended_items_slim = self.slim.s_recommend(user, nRec+extra)
                weighting_dict['slim'] = (recommended_items_slim, self.slim_weight)

            if (self.enableP3A):
                recommended_items_p3a = self.p3a.s_recommend(user, nRec + extra)
                weighting_dict['p3a'] = (recommended_items_p3a, self.p3a_weight)

            return self.item_weighter(weighting_dict, nRec, extra)

        elif self.method == 'rating_weight':

            norm_method = self.norm

            recommended_items_user = self.normalize_row(self.cbu.get_pred_row(user), method=norm_method)
            recommended_items_item = self.normalize_row(self.cbi.get_pred_row(user), method=norm_method)
            recommended_items_cbf = self.normalize_row(self.cbf.get_pred_row(user), method=norm_method)

            pred_row_sparse = recommended_items_user * self.user_weight + recommended_items_item * self.item_weight \
                              + recommended_items_cbf * self.cbf_weight

            if self.enableSLIM:
                recommended_items_slim = self.normalize_row(self.getSlimRow(user), method=norm_method)
                pred_row_sparse = pred_row_sparse + self.slim_weight * recommended_items_slim

            if self.enableCBF2:
                recommended_items_cbf2 = self.normalize_row(self.cbf2.get_pred_row(user), method=norm_method)
                pred_row_sparse = pred_row_sparse + self.cbf2_weight * recommended_items_cbf2

            if self.enableP3A:
                row = self.p3a.get_pred_row(user)
                pred_row_sparse = pred_row_sparse + self.p3a_weight * row

            if self.enableRP3B:
                row = self.rp3b.get_pred_row(user)
                pred_row_sparse = pred_row_sparse + self.rp3b_weight * row


            pred_row = np.array(pred_row_sparse.todense()).squeeze()

            if self.enableLFM:
                recommended_items_lfm = self.normalize_row(self.lfm.get_pred_row(user), method=norm_method)
                pred_row = pred_row + self.lfm_weight * recommended_items_lfm

            if self.enableSVD:
                recommended_items_svd = self.normalize_row(self.svd.get_pred_row(user), method=norm_method)
                pred_row = pred_row + self.svd_weight * recommended_items_svd

            ranking = np.argsort(-pred_row)
            recommended_items = self._filter_seen(user, ranking)

            return recommended_items[0:nRec]

        elif self.method == "hybrid":

            norm_method = 'max'
            extra = 1

            recommended_items_user = self.normalize_row(self.cbu.get_pred_row(user), method=norm_method)
            recommended_items_item = self.normalize_row(self.cbi.get_pred_row(user), method=norm_method)
            recommended_items_cbf = self.normalize_row(self.cbf.get_pred_row(user), method=norm_method)

            recommended_items_cbf2 = None
            if (self.enableCBF2):
                recommended_items_cbf2 = self.normalize_row(self.cbf2.get_pred_row(user), method=norm_method)

            recommended_items_rp3b = None
            if (self.enableRP3B):
                recommended_items_rp3b = self.normalize_row(self.rp3b.get_pred_row(user), method=norm_method)

            recommended_items_slim = None
            if (self.enableSLIM):
                recommended_items_slim = self.normalize_row(self.getSlimRow(user), method=norm_method)

            weighting_dict = {}



            return self.item_weighter(weighting_dict, nRec, extra)




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

    def item_weighter(self, tupleDict, nRec, extra):

        # initialize a dict with recommended items as keys and value zero
        result = {}
        for tuple in tupleDict.values():

            items = tuple[0]

            for i in range(nRec + extra):
                result[str(items[i])] = 0


        # assign a score based on position

        for tuple in tupleDict.values():

            items = tuple[0]
            weight = tuple[1]

            for i in range(nRec + extra):
                result[str(items[i])] += (nRec+extra - i) * weight

        # sort the dict
        sorted_results = sorted(result.items(), key=itemgetter(1))
        rec_items = [x[0] for x in sorted_results]

        # flip to order by decreasing order
        rec_items = rec_items[::-1]

        # return only the topN recommendations
        return np.array(rec_items[0:nRec]).astype(int)

    def predWeightRatingRows(self, user, nRec, recommended_items_user, recommended_items_item,
                             recommended_items_cbf, recommended_items_cbf2,
                             recommended_items_rp3b, recommended_items_slim):


        pred_row_sparse = recommended_items_user * self.user_weight + recommended_items_item * self.item_weight \
                    + recommended_items_cbf * self.cbf_weight


        if self.enableSLIM and self.method != "hybrid":
            pred_row_sparse = pred_row_sparse + self.slim_weight * recommended_items_slim

        if self.enableCBF2:
            pred_row_sparse = pred_row_sparse + self.cbf2_weight * recommended_items_cbf2

        # needs to be before rp3b because rp3b output is dense
        pred_row = np.array(pred_row_sparse.todense()).squeeze()

        if self.enableRP3B:
            pred_row = pred_row + self.rp3b_weight * recommended_items_rp3b


        ranking = np.argsort(-pred_row)
        recommended_items = self._filter_seen(user, ranking)

        return recommended_items[0:nRec]

    def _filter_seen(self, user_id, ranking):
        seen = self.urm.extractTracksFromPlaylist(user_id)
        unseen_mask = np.in1d(ranking, seen, assume_unique=True, invert=True)
        return ranking[unseen_mask]

    def getSlimRow(self, user):
        return self.urm.getCSR().getrow(user) * self.slim_sim

    def setEnables(self, enable_dict):
        self.enableSVD = enable_dict.get('enableSVD')
        self.enableRP3B = enable_dict.get('enableRP3B')
        self.enableSLIM = enable_dict.get('enableSLIM')
        self.enableCBF2 = enable_dict.get('enableCBF2')
        self.enableP3A  = enable_dict.get('enableP3A')
        self.enableLFM = enable_dict.get('enableLFM')

    def normalize_row(self, recommended_items , method):
        if method == 'max':
            norm_factor = recommended_items.max()
            if norm_factor == 0: norm_factor = 1
            return recommended_items / norm_factor

        elif method == 'sum':
            norm_factor = recommended_items.sum()
            if norm_factor == 0: norm_factor = 1
            return recommended_items / norm_factor

        elif method == 'l1':

            return normalize(recommended_items, norm='l1')

        elif method == 'l2':
            return normalize(recommended_items, norm='l2')
        else:
            raise ValueError('Not a valid normalization method')



