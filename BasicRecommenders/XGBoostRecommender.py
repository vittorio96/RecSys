import pandas as pd

from BasicRecommenders.CollaborativeFiltering import CollaborativeFiltering
from BasicRecommenders.SVDRecommender import SVDRecommender
from HybridRecommenders.ItemItemHybridRecommender import IIHybridRecommender
from BasicRecommenders.LightFMRecommender import LightFMRecommender
from BasicRecommenders.P3alpha import P3alpha
from SLIM_BPR2.Cython.SLIM_BPR_Cython import SLIM_BPR_Cython


class XGBoostRecommender():

    def __init__(self, urm, urm_t, icm, icm2, enable_dict, urm_test):
        self.urm = urm
        self.n_users, self.n_items = urm.getCSR().shape
        self.setEnables(enable_dict )

        self.item_item = IIHybridRecommender(urm, icm, icm2)
        self.item_item.fit(item_weight=0.4, cbf1_weight=0.25, cbf2_weight=0.1)

        self.user = CollaborativeFiltering()
        self.user.fit(urm_t, k=100, h=0, mode='user')

        if self.enableSVD:
            self.svd = SVDRecommender(urm, nf=385)

        if self.enableP3A:
            self.p3a = P3alpha(urm.getCSR())
            self.p3a.fit(topK=80, alpha=1, min_rating=0, implicit=True, normalize_similarity=True)

        if self.enableSLIM:
            choice = 2
            logFile = open("SLIM_BPR_Cython.txt", "a")

            self.slim = SLIM_BPR_Cython(urm.getCSR(), recompile_cython=False, positive_threshold=0,
                                        URM_validation=urm_test.getCSR(), final_model_sparse_weights=True,
                                        train_with_sparse_weights=False)

            self.slim.fit(epochs=100, validation_every_n=1, logFile=logFile, batch_size=5, topK=200,
                          sgd_mode="adagrad", learning_rate=0.075)

        if self.enableLFM:
            # LightFM
            print("starting USER CF")
            self.lfm = LightFMRecommender()
            self.lfm.fit(urm, epochs=100)
            print("USER CF finished")


    def buildXGBoostMatrix(self, recommenders, n):

        print("building XGBoost Matrix")
        user_id_col     = []
        slim_rec_col    = []
        itit_rec_col    = []
        p3a_rec_col     = []
        svd_rec_col     = []
        user_rec_col    = []
        lfm_rec_col     = []
        prof_len_col    = []

        for user in range(self.n_users):

            # Item Item
            itit_rec = self.item_item.s_recommend(user, n).tolist()
            user_id_col.extend(itit_rec)
            itit_rec_col.extend([user] * len(itit_rec))

            # User
            user_rec = self.user.g(user, n)
            user_id_col.extend(user_rec)
            user_rec_col.extend([user] * len(user_rec))

            # P3A
            if self.enableP3A:
                p3a_rec = self.p3a.s_recommend(user, n)
                user_id_col.extend(p3a_rec)
                p3a_rec_col.extend([user] * len(p3a_rec))

            # SVD
            if self.enableSVD:
                svd_rec = self.svd.s_recommend(user, n)
                user_id_col.extend(svd_rec)
                svd_rec_col.extend([user] * len(svd_rec))

            # LFM
            if self.enableLFM:
                lfm_rec = self.lfm.s_recommend(user, n)
                user_id_col.extend(lfm_rec)
                lfm_rec_col.extend([user] * len(lfm_rec))

            # SLIM
            if self.enableSLIM:
                slim_rec = self.slim.s_recommend(user, n)
                user_id_col.extend(slim_rec)
                slim_rec_col.extend([user] * len(slim_rec))

            # Profile Len
            profileLen = len(self.urm.extractTracksFromPlaylist(user))
            prof_len_col.extend([profileLen] * len(user_rec))

            dict = {"user_id": user_id_col,
                    "itit_rec_id": itit_rec_col,
                    "user_rec_id": user_rec_col}
                    # "slim_rec_id": slim_rec_col,
                    # "p3a_rec_id": p3a_rec_col,
                    # "lfm_rec_id": lfm_rec_col,
                    # "svd_rec_id": svd_rec_col,
                    # "profile_len": prof_len_col}

            self.buildDataFrame(dict)

    def setEnables(self, enable_dict):
        self.enableSVD  = enable_dict.get('enableSVD')
        self.enableSLIM = enable_dict.get('enableSLIM')
        self.enableCBF2 = enable_dict.get('enableCBF2')
        self.enableP3A  = enable_dict.get('enableP3A')
        self.enableLFM  = enable_dict.get('enableLFM')

    def buildDataFrame(self, dict):
        print("building dataframe")
        self.df = pd.DataFrame(dict)
        self.df.describe()
        print("built df")
        # dtrain = xgb.DMatrix(df, label=y_train)


