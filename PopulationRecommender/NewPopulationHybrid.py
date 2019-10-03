import numpy as np
from sklearn.preprocessing import normalize
from tqdm import tqdm

from BasicRecommenders.CF_TFIDF import CollaborativeFiltering_TFIDF
from BasicRecommenders.CollaborativeFiltering import CollaborativeFiltering
from BasicRecommenders.ImprovedCBF import ImprovedCBF
from BasicRecommenders.RP3beta import RP3betaRecommender
from MatrixFactorization.MatrixFactorization_RMSE import IALS_numpy
from MatrixFactorization.PureSVD import PureSVDRecommender
from SLIM_BPR2.Cython.SLIM_BPR_Cython import SLIM_BPR_Cython


class NewHybrid(object):

        def __init__(self, matrices, param_dict, enable_dict):

            print("Fitting Hybrid...")

            self.urm = matrices.get('URM')
            self.urm_t = matrices.get('URM_T')
            self.icm1 = matrices.get('ICM_1')
            self.icm2 = matrices.get('ICM_2')

            self.param_dict = param_dict
            self.weight_dict = param_dict.get('weight_dict')
            self.setEnables(enable_dict)
            self.setWeights(self.weight_dict)

            self.buildmodel()


        def buildmodel(self):
            if self.enableCBI:
                print("Fitting Item CF...")
                self.cbi = CollaborativeFiltering()
                self.cbi.fit(self.urm, **self.param_dict.get('cbi_param_dict'))
                print("Item CF finished")

            if self.enableRP3B:
                print("Fitting RP3B...")
                self.rp3b = RP3betaRecommender(self.urm.getCSR())
                self.rp3b.fit(**self.param_dict.get('rp3b_param_dict'))
                print("RP3B finished")

            if self.enableCBF:
                self.cbf = ImprovedCBF(self.icm1, self.icm2, self.urm, **self.param_dict.get('cbf_param_dict'))
                self.cbf.fit(self.param_dict.get('CBFNorm'))
                print("CBF finished")

            if self.enableCBU:
                self.cbu = CollaborativeFiltering()
                self.cbu.fit(self.urm_t, **self.param_dict.get('cbu_param_dict'))
                print("USER CF finished")

            if self.enableSLIM:
                self.loadSLIM = self.param_dict.get('loadSLIM')
                self.slimPath = self.param_dict.get('slimPath')

                self.slim = SLIM_BPR_Cython(self.urm.getCSR(), recompile_cython=False, positive_threshold=0,
                                            final_model_sparse_weights=True,
                                            train_with_sparse_weights=False)

                if self.loadSLIM :
                    print("Loading matrix")
                    self.slim.loadModel('',self.slimPath)

                else:
                    print("Calculating similarity matrix")
                    logFile = open("SLIM_BPR_Cython.txt", "a")
                    self.slim.fit(**self.param_dict.get('slim_param_dict'))
                    self.slim.saveModel('',self.slimPath)

                self.normalizeSLIM = self.param_dict.get('normalizeSLIM')

                if self.normalizeSLIM != None :
                    self.slim_sim = normalize(self.slim.get_similarity(), norm=self.normalizeSLIM, axis=1)
                else:
                    self.slim_sim = self.slim.get_similarity()

            if self.enableSVD:
                self.loadSVD = self.param_dict.get('loadSVD')
                self.svdPath = self.param_dict.get('svdPath')

                self.svd = IALS_numpy(**self.param_dict.get('svd_param_dict'))

                if self.loadSVD:
                     print("Loading svd")
                     self.svd.loadModel(self.svdPath)
                     self.svd.set_dataset(self.urm.getCSR())
                else:
                     print("Calculating svd")
                     self.svd.fit(self.urm.getCSR())
                     self.svd.saveModel(self.svdPath)

            print("Fitting Hybrid done ")

        def setEnables(self, enable_dict):
            self.enableCBF = enable_dict.get('enableCBF')
            self.enableCBI = enable_dict.get('enableCBI')
            self.enableRP3B = enable_dict.get('enableRP3B')
            self.enableCBU = enable_dict.get('enableCBU')
            self.enableSLIM = enable_dict.get('enableSLIM')
            self.enableSVD = enable_dict.get('enableSVD')
            self.enableSLEN = enable_dict.get('enableSLEN')


        def s_recommend(self, user, nRec=10):

            pred_row_sparse = 0
            norm_method = "max"

            if self.enableCBI:
                pred_row_sparse = pred_row_sparse + self.normalize_row(self.cbi.get_pred_row(user), norm_method) * self.cbi_weight

            if self.enableCBU:
                pred_row_sparse = pred_row_sparse + self.normalize_row(self.cbu.get_pred_row(user), norm_method) * self.cbu_weight

            if self.enableCBF:
                pred_row_sparse = pred_row_sparse + self.normalize_row(self.cbf.get_pred_row(user), norm_method) * self.cbf_weight

            if self.enableRP3B:
                pred_row_sparse = pred_row_sparse + self.normalize_row(self.rp3b.get_pred_row(user),norm_method) * self.rp3b_weight

            if self.enableSLIM:
                pred_row_sparse = pred_row_sparse + self.normalize_row(self.get_pred_row_slim(user),norm_method) * self.slim_weight

            if self.enableSLEN:
                pred_row_sparse = pred_row_sparse + self.normalize_row(self.slim_en.get_pred_row(user),norm_method) * self.slen_weight

            pred_row = np.array(pred_row_sparse.todense()).squeeze()

            #After it is dense add svd
            if self.enableSVD:
                pred_row = pred_row + self.normalize_row(self.svd.get_pred_row(user),norm_method) * self.svd_weight

            ranking = np.argsort(-pred_row)

            recommended_items = self._filter_seen(user, ranking)

            return recommended_items[0:nRec]


        def _filter_seen(self, user_id, ranking):
            seen = self.urm.extractTracksFromPlaylist(user_id)
            unseen_mask = np.in1d(ranking, seen, assume_unique=True, invert=True)
            return ranking[unseen_mask]

        def normalize_row(self, recommended_items, method):
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

            elif method == 'none':
                return recommended_items

            else:
                raise ValueError('Not a valid normalization method')

        def setWeights(self, weight_dict):
            self.cbi_weight = weight_dict.get('cbi_weight')
            self.rp3b_weight = weight_dict.get('rp3b_weight')
            self.cbf_weight = weight_dict.get('cbf_weight')
            self.cbu_weight = weight_dict.get('cbu_weight')
            self.slim_weight = weight_dict.get('slim_weight')
            self.svd_weight = weight_dict.get('svd_weight')
            self.slen_weight = weight_dict.get('slen_weight')

        def get_pred_row_slim(self, user):
            return self.urm.getCSR().getrow(user).dot(self.slim_sim)

        def m_recommend(self, target_ids, nRec=10):
            results = []
            for tid in tqdm(target_ids):
                results.append(self.s_recommend(tid, nRec))
            return results


class NewPopulationHybrid(object):

    def __init__(self, matrices, groups, groups_param_dict, enable_dict):

        self.group1 = groups[0]
        self.group2 = groups[1]
        self.group3 = groups[2]
        self.enabled_groups = enable_dict.get('enabled_groups')

        if '1' in self.enabled_groups:
            self.group1_hybrid = NewHybrid(matrices, groups_param_dict[0], enable_dict)

        if '2' in self.enabled_groups:
            self.group2_hybrid = NewHybrid(matrices, groups_param_dict[1], enable_dict)

        if '3' in self.enabled_groups:
            self.group3_hybrid = NewHybrid(matrices, groups_param_dict[2], enable_dict)

    def s_recommend(self, user, nRec=10):

        if '1' in   self.enabled_groups and np.isin(user, self.group1, assume_unique=True):
            return self.group1_hybrid.s_recommend(user, nRec)

        elif '2' in self.enabled_groups and np.isin(user, self.group2, assume_unique=True):
            return self.group2_hybrid.s_recommend(user, nRec)

        elif '3' in self.enabled_groups and np.isin(user, self.group3, assume_unique=True):
            return self.group3_hybrid.s_recommend(user, nRec)

        else:
            return np.zeros(10)


    def m_recommend(self, target_ids, nRec=10):
        results = []
        for tid in tqdm(target_ids):
            results.append(self.s_recommend(tid, nRec))
        return results

    def setWeights(self, group, weight_dict):
        if group == 1 and '1' in self.enabled_groups:
            self.group1_hybrid.setWeights(weight_dict)
        elif group == 2 and '2' in self.enabled_groups:
            self.group2_hybrid.setWeights(weight_dict)
        elif group == 3 and '3' in self.enabled_groups:
            self.group3_hybrid.setWeights(weight_dict)
        else:
            raise ModuleNotFoundError("Not an enabled group")