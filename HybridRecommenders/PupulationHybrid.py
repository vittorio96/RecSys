from operator import itemgetter

from BasicRecommenders.ContentBasedFiltering import ContentBasedFiltering
from BasicRecommenders.SVDRecommender import SVDRecommender
from BasicRecommenders.CollaborativeFiltering import CollaborativeFiltering

import numpy as np

from SLIM_BPR2.Cython.SLIM_BPR_Cython import SLIM_BPR_Cython




class PopulationHybrid():

    def __init__(self, urm, urm_t, icm, icm2, enable_dict, param_dict, urm_test=None):
        self.urm = urm
        self.setEnables(enable_dict)

        self.group_1_params = param_dict.get('group_1_params')
        self.group_2_params = param_dict.get('group_2_params')
        self.group_1_2_TH = param_dict.get('group_1_2_TH')

        if self.enableSVD:
            self.svd = SVDRecommender(urm, nf=385)


        if self.enableSLIM:
            logFile = open("SLIM_BPR_Cython.txt", "a")


            self.slim = SLIM_BPR_Cython(urm.getCSR(), recompile_cython=False, positive_threshold=0,
                                   URM_validation=urm_test.getCSR(),final_model_sparse_weights=True,
                                   train_with_sparse_weights=False)

            self.slim.fit(epochs=200, validation_every_n=1, logFile=logFile, batch_size=5, topK=200,
                         sgd_mode="adagrad", learning_rate=0.075)

            self.slim_sim = self.slim.get_similarity()



        # User based
        print("starting USER CF")
        self.cbu = CollaborativeFiltering()
        self.cbu.fit(urm_t, k=100, h=8, mode='user')
        print("USER CF finished")

        # Item based
        print("starting ITEM CF")
        self.cbi = CollaborativeFiltering()
        self.cbi.fit(urm, k=125, h=10, mode='item')
        print("ITEM CF finished")

        # Content based artist
        print("starting CBF")
        self.cbf = ContentBasedFiltering(icm, urm, k=25, shrinkage=0)
        self.cbf.fit()
        print("CBF finished")

        if self.enableCBF2:
            print("starting CBF2")
            self.cbf2 = ContentBasedFiltering(icm2, urm, k=25, shrinkage=0)
            self.cbf2.fit()
            print("CBF2 finished")


    def changeParams(self, param_dict):
        self.group_1_params = param_dict.get('group_1_params')
        self.group_2_params = param_dict.get('group_2_params')
        self.group_1_2_TH   = param_dict.get('group_1_2_TH')

    def fit(self, weights_dict, method='rating_weight'):

        self.user_weight = weights_dict.get('user_weight',0)
        self.item_weight = weights_dict.get('item_weight',0)
        self.cbf_weight  = weights_dict.get('cbf_weight' ,0)
        self.cbf2_weight = weights_dict.get('cbf2_weight',0)
        self.svd_weight  = weights_dict.get('svd_weight' ,0)
        self.slim_weight = weights_dict.get('slim_weight',0)

        self.method = method

    def s_recommend(self, user, nRec=10):

        number_items = len(self.urm.extractTracksFromPlaylist(user))
        if number_items > self.group_1_2_TH:
            self.fit(self.group_2_params)
        else:
            self.fit(self.group_1_params)

        if self.method == 'item_weight':
            extra = 1

            recommended_items_user = self.cbu.s_recommend(user, nRec+extra)
            recommended_items_item = self.cbi.s_recommend(user, nRec+extra)
            recommended_items_cbf = self.cbf.s_recommend(user, nRec+extra)

            weighting_dict = {'user': (recommended_items_user, self.user_weight),
                              'item': (recommended_items_item, self.item_weight),
                              'cbf' : (recommended_items_cbf , self.cbf_weight)}

            recommended_items_cbf2 = None
            if (self.enableCBF2):
                recommended_items_cbf2 = self.cbf2.s_recommend(user, nRec+extra)
                weighting_dict['cbf2'] = (recommended_items_cbf2, self.cbf2_weight)

            recommended_items_svd = None
            if (self.enableSVD):
                recommended_items_svd = self.svd.s_recommend(user, nRec+extra)
                weighting_dict['svd'] = (recommended_items_svd, self.svd_weight)

            recommended_items_slim = None
            if (self.enableSLIM):
                recommended_items_slim = self.slim.s_recommend(user, nRec+extra)
                weighting_dict['slim'] = (recommended_items_slim, self.slim_weight)

            return self.item_weighter(weighting_dict, nRec, extra)

        elif self.method == 'rating_weight':

            norm_method = 'max'

            recommended_items_user = self.normalize_row(self.cbu.get_pred_row(user), method=norm_method)
            recommended_items_item = self.normalize_row(self.cbi.get_pred_row(user), method=norm_method)
            recommended_items_cbf = self.normalize_row(self.cbf.get_pred_row(user), method=norm_method)

            recommended_items_cbf2 = None
            if (self.enableCBF2):
                recommended_items_cbf2 = self.normalize_row(self.cbf2.get_pred_row(user), method=norm_method)

            recommended_items_svd=None
            if(self.enableSVD):
                recommended_items_svd = self.normalize_row(self.svd.get_pred_row(user), method=norm_method)

            recommended_items_slim = None
            if (self.enableSLIM):
                recommended_items_slim = self.normalize_row(self.getSlimRow(user), method=norm_method)


            return self.predWeightRatingRows(user, nRec, recommended_items_user, recommended_items_item,
                                             recommended_items_cbf, recommended_items_cbf2, recommended_items_svd,
                                             recommended_items_slim)

        elif self.method == "hybrid":

            norm_method = 'max'
            extra = 1

            recommended_items_user = self.normalize_row(self.cbu.get_pred_row(user), method=norm_method)
            recommended_items_item = self.normalize_row(self.cbi.get_pred_row(user), method=norm_method)
            recommended_items_cbf = self.normalize_row(self.cbf.get_pred_row(user), method=norm_method)

            recommended_items_cbf2 = None
            if (self.enableCBF2):
                recommended_items_cbf2 = self.normalize_row(self.cbf2.get_pred_row(user), method=norm_method)

            recommended_items_svd = None
            if (self.enableSVD):
                recommended_items_svd = self.normalize_row(self.svd.get_pred_row(user), method=norm_method)

            recommended_items_slim = None
            if (self.enableSLIM):
                recommended_items_slim = self.normalize_row(self.getSlimRow(user), method=norm_method)

            weighting_dict = {}

            weighting_dict['hybrid'] =(self.predWeightRatingRows(user, nRec+extra, recommended_items_user, recommended_items_item,
                                             recommended_items_cbf, recommended_items_cbf2, recommended_items_svd,
                                             recommended_items_slim), self.hybrid_ensemble_weight)

            recommended_items_slim = self.slim.s_recommend(user, nRec+extra)
            weighting_dict['slim'] = (recommended_items_slim, self.hybrid_slim_weight)

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
                             recommended_items_svd, recommended_items_slim):


        pred_row_sparse = recommended_items_user * self.user_weight + recommended_items_item * self.item_weight \
                    + recommended_items_cbf * self.cbf_weight


        if self.enableSLIM and self.method != "hybrid":
            pred_row_sparse = pred_row_sparse + self.slim_weight * recommended_items_slim

        if self.enableCBF2:
            pred_row_sparse = pred_row_sparse + self.cbf2_weight * recommended_items_cbf2

        # needs to be before svd because svd output is dense
        pred_row = np.array(pred_row_sparse.todense()).squeeze()

        if self.enableSVD:
            pred_row = pred_row + self.svd_weight * recommended_items_svd


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



