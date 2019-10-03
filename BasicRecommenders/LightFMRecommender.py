import numpy as np
from lightfm import LightFM


class LightFMRecommender(object):

    def __init__(self, n_comp=30, loss ='warp-kos', learning = 'adagrad', alpha=1e-3):
        alpha = 1e-3
        self.model = LightFM(no_components=30,
                                loss='warp-kos',
                                learning_schedule='adagrad',
                                user_alpha=alpha, item_alpha=alpha)

        # self.model = LightFM(no_components=n_comp,
        #                 loss=loss,
        #                 learning_schedule= learning,
        #                 user_alpha=alpha, item_alpha=alpha)

    def fit(self, urm, epochs=100):
        self.urm = urm
        self.n_tracks = urm.shape[1]
        for epoch in range(epochs):
            self.model.fit_partial(urm.getCSR(), epochs=1)


    def get_pred_row(self, user_id):
        return self.model.predict(user_id, np.arange(self.n_tracks))

    def s_recommend(self, user_id, nRec=10):
        scores = self.model.predict(user_id, np.arange(self.n_tracks))
        top_items = np.argsort(-scores)

        recommended_items = self._filter_seen(user_id, top_items)
        return recommended_items[0:nRec]


    def _filter_seen(self, user_id, ranking):
        seen = self.urm.extractTracksFromPlaylist(user_id)
        unseen_mask = np.in1d(ranking, seen, assume_unique=True, invert=True)
        return ranking[unseen_mask]

    def m_recommend(self, target_ids, nRec=10):
        results = []
        for tid in target_ids:
            results.append(self.s_recommend(tid, nRec))
        return results

