import pickle
from operator import itemgetter
import numpy as np

from BasicRecommenders.CollaborativeFiltering import CollaborativeFiltering
from BasicRecommenders.RP3beta import RP3betaRecommender
from BasicRecommenders.P3alpha import P3alpha
from SLIM_BPR2.Cython.SLIM_BPR_Cython import SLIM_BPR_Cython
from HybridRecommenders.ItemItemHybridRecommender import IIHybridRecommender


class ListHybridRecommender():

    def __init__(self, urm, urm_t, icm, icm2, enable_dict, urm_test, recalcSLIM=True):
        self.urm = urm
        self.setEnables(enable_dict )

        self.item_item = IIHybridRecommender(urm, icm, icm2)
        self.item_item.fit(item_weight=0.4, cbf1_weight=0.25, cbf2_weight=0.1)

        if self.enableUSER:
            self.cbu = CollaborativeFiltering()
            self.cbu.fit(urm_t, k=100, h=0, mode='user')

        if self.enableRP3B:

            self.rp3b = RP3betaRecommender(urm.getCSR())

        if self.enableP3A:
            self.p3a = P3alpha(urm.getCSR())
            self.p3a.fit(topK=80, alpha=1, min_rating=0, implicit=True, normalize_similarity=True)

        if self.enableSLIM:
            if recalcSLIM:
                choice = 2
                logFile = open("SLIM_BPR_Cython.txt", "a")

                self.slim = SLIM_BPR_Cython(urm.getCSR(), recompile_cython=False, positive_threshold=0,
                                            URM_validation=urm_test.getCSR(), final_model_sparse_weights=True,
                                            train_with_sparse_weights=False)

                self.slim.fit(epochs=100, validation_every_n=1, logFile=logFile, batch_size=5, topK=200,
                              sgd_mode="adagrad", learning_rate=0.075)

                self.slim_sim = self.slim.get_similarity()

                # with open('slim_sub.pkl', 'wb') as output:
                #     pickle.dump(self.slim, output, pickle.HIGHEST_PROTOCOL)

            else:
                with open('slim_test.pkl', 'rb') as input:
                    self.slim = pickle.load(input)

    def fit(self, weights_dict= None, norm="none", w_method = "count"):
        self.norm_method = norm
        self.weights_dict = weights_dict
        self.w_method = w_method

        self.item_item_weight = weights_dict.get('item_item_weight', 0)
        self.rp3b_weight = weights_dict.get('rp3b_weight', 0)
        self.slim_weight = weights_dict.get('slim_weight', 0)
        self.user_weight = weights_dict.get('user_weight', 0)
        self.p3a_weight = weights_dict.get('p3a_weight', 0)

    def s_recommend(self, user, nRec=10):

        weighting_dict = {}

        #recommended_items_item_item = self.normalize_row(self.item_item.get_pred_row(user), method=self.norm_method)
        recommended_items_item_item = self.item_item.s_recommend(user,nRec).tolist()
        weighting_dict['ii'] = (recommended_items_item_item, self.item_item_weight)

        recommended_items_rp3b = None
        if(self.enableSVD):
            #recommended_items_rp3b = self.normalize_row(self.svd.get_pred_row(user), method=self.norm_method)
            recommended_items_rp3b = self.rp3b.s_recommend(user,nRec).tolist()
            weighting_dict['rp3b'] = (recommended_items_rp3b, self.rp3b_weight)

        recommended_items_p3a = None
        if (self.enableP3A):
            # recommended_items_svd = self.normalize_row(self.svd.get_pred_row(user), method=self.norm_method)
            recommended_items_p3a = self.p3a.s_recommend(user, nRec)
            weighting_dict['p3a'] = (recommended_items_p3a, self.p3a_weight)

        recommended_items_user = None
        if (self.enableUSER):
            recommended_items_user = self.cbu.s_recommend(user, nRec).tolist()
            weighting_dict['user'] = (recommended_items_user, self.user_weight)

        recommended_items_slim = None
        if (self.enableSLIM):
            #recommended_items_slim = self.normalize_row(self.getSlimRow(user), method=self.norm_method)
            recommended_items_slim = self.slim.s_recommend(user,nRec)
            weighting_dict['slim'] = (recommended_items_slim, self.slim_weight)

        return self.list_weighter(weighting_dict, nRec, 0, self.w_method)
        #return list_merger(weighting_dict, nRec)

    def m_recommend(self, user_ids, nRec=10):

        results = []
        for uid in user_ids:
            results.append(self.s_recommend(uid, nRec))
        return results

    def list_weighter(self, tupleDict, nRec, extra, weighting = 'parab'):
        """
            :param tupleDict : dict{(list_of_items, weight)}
                                assumes list_of_items is ordered from best rec
                                to worst rec

            :param nRec      : number of items to recommend

            :param extra     : number of extra_items to consider
                               in the lists

            :param weighting : - "linear" 1st place 10, 2nd place 9 ...
                                10th place 1
                               - "parab" 1st place 10,..  5th place 3.5 ...
                                10th place 1

            :return list of nRec items weighted according to dict
        """

        # initialize a dict with items as keys and starting value zero
        result = {}
        count_dict = {}
        for tuple in tupleDict.values():

            items = tuple[0]

            for i in range(nRec + extra):
                result[str(items[i])] = 0
                count_dict[str(items[i])] = 0

        # assign a score based on position
        for tuple in tupleDict.values():

            items = tuple[0]
            weight = tuple[1]

            # weighting logic
            if weighting == 'linear':
                for i in range(nRec + extra):
                    result[str(items[i])] += (nRec+extra - i) * weight

            elif weighting == 'parab':
                for i in range(nRec + extra):
                    result[str(items[i])] += (0.1 * i**2 - 1.92 * i + nRec) * weight

            elif weighting == 'avg':
                for i in range(nRec + extra):
                    result[str(items[i])] += (nRec - i)/3

            elif weighting == 'count_par':

                for i in range(nRec + extra):
                    count_dict[str(items[i])] += 1

                for i in range(nRec + extra):
                    result[str(items[i])] += (0.1 * i ** 2 - 1.92 * i + nRec) * weight \
                                             + 4 * count_dict.get(str(items[i]))

            else: raise ValueError('Not a valid weighting logic')

        # sort the dict
        sorted_results = sorted(result.items(), key=itemgetter(1))
        rec_items = [x[0] for x in sorted_results]

        # flip to order by decreasing order
        rec_items = rec_items[::-1]

        # return only the topN recommendations
        return np.array(rec_items[0:nRec]).astype(int)

    def setEnables(self, enable_dict):
        self.enableSVD = enable_dict.get('enableSVD')
        self.enableSLIM = enable_dict.get('enableSLIM')
        self.enableUSER = enable_dict.get('enableUSER', False)
        self.enableP3A = enable_dict.get('enableP3A', False)

    def _filter_seen(self, user_id, ranking):
        user_profile = self.urm.getCSR()[user_id]
        seen = user_profile.indices
        unseen_mask = np.in1d(ranking, seen, assume_unique=True, invert=True)
        return ranking[unseen_mask]

    def normalize_row(self, recommended_items , method):
        if method == 'max':
            norm_factor = recommended_items.max()
            if norm_factor == 0: norm_factor = 1
            return recommended_items / norm_factor

        elif method == 'sum':
            norm_factor = recommended_items.sum()
            if norm_factor == 0: norm_factor = 1
            return recommended_items / norm_factor

        elif method == "none":
            return recommended_items
        else:
            raise ValueError('Not a valid normalization method')

    def getSlimRow(self, user):
        return self.urm.getCSR().getrow(user) * self.slim_sim

    def remove_duplicates(self, ordered_list):
        """
        :param ordered_list
        :return: the ordered_list still ordered removed of duplicates
        """
        seen = set()
        seen_add = seen.add
        return [x for x in ordered_list if not (x in seen or seen_add(x))]




# # TESTS
# wl ={}
# wl['a'] = (['20','30','40','21','31','41','22','32','42','23','33','44'], 1)
# list_weighter(wl, 10, 0, weighting="parab")
