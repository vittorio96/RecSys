import numpy as np
from tqdm import tqdm


def precision(recommended_items, relevant_items):
    is_relevant = np.isin(recommended_items, relevant_items, assume_unique=True)

    precision_score = np.sum(is_relevant, dtype=np.float32) / len(is_relevant)

    return precision_score

def recall(recommended_items, relevant_items):
    is_relevant = np.isin(recommended_items, relevant_items, assume_unique=True)

    recall_score = np.sum(is_relevant, dtype=np.float32) / relevant_items.shape[0]

    return recall_score

def MAP(recommended_items, relevant_items):
    is_relevant = np.isin(recommended_items, relevant_items, assume_unique=True)

    # Cumulative sum: precision at 1, at 2, at 3 ...
    p_at_k = is_relevant * np.cumsum(is_relevant, dtype=np.float32) / (1 + np.arange(is_relevant.shape[0]))

    map_score = np.sum(p_at_k) / np.min([relevant_items.shape[0], is_relevant.shape[0]])

    return map_score

def evaluate_algorithm(URM_test, recommender_object, h=None, uw=1, iw=1, st=10):
    print("Evaluating")
    cumulative_precision = 0.0
    cumulative_recall = 0.0
    cumulative_MAP = 0.0

    num_eval = 0

    usersList = URM_test.getPlaylists()

    for user_id in usersList:

        relevant_items = URM_test.extractTracksFromPlaylist(user_id)  # I extract the relevant tracks for that playlist

        if len(relevant_items) > 0:

            if h == 'weight':
                recommended_items = recommender_object.s_recommend(user_id, method=h, user_weight=uw, item_weight=iw)
            elif h == 'switch':
                recommended_items = recommender_object.s_recommend(user_id, methods=h, switchTH=st)
            else:
                recommended_items = recommender_object.s_recommend(user_id)

            if not np.array_equal(recommended_items, np.zeros(10)):
                num_eval += 1

                cumulative_precision += precision(recommended_items, relevant_items)
                cumulative_recall += recall(recommended_items, relevant_items)
                cumulative_MAP += MAP(recommended_items, relevant_items)

    cumulative_precision /= num_eval
    cumulative_recall /= num_eval
    cumulative_MAP /= num_eval

    return (cumulative_precision, cumulative_recall, cumulative_MAP)