import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

from BasicRecommenders.RP3beta import RP3betaRecommender
from RecKit.URM import URM
from RecKit.evaluation_tools import evaluate_algorithm


def crossValidate(logFile, test_size=0.01) :
    """
    :param recommender: we assume it has an s_recommend method
    :param test_size: how big is the test size in the test split
    :return: prints to screen and to logfile the cv average of the metrics
    """
    Kaggle = False

    if Kaggle == True:
        interactionsCsv = pd.read_csv("../input/train.csv")
        targetList = pd.read_csv("../input/target_playlists.csv").iloc[:, 0]
        tracksCsv = pd.read_csv("../input/tracks.csv")
    else:
        interactionsCsv = pd.read_csv("input/train.csv")
        targetList = pd.read_csv("input/target_playlists.csv").iloc[:, 0]
        tracksCsv = pd.read_csv("input/tracks.csv")

    cumulative_precision = [None] * 8
    cumulative_recall = [None] * 8
    cumulative_MAP = [None] * 8


    cf_parameters = {'topK': 80,
                         'alpha': 1,
                         'beta': 0.27,
                         'normalize_similarity': True,
                         'implicit': True,
                         'norm': 'l1'}

    for seed, i in zip([13, 17, 23, 33, 45, 57, 69, 77], range(8)):

        X_train, X_test = train_test_split(interactionsCsv, test_size=test_size, random_state=seed)
        urm_train = URM(X_train)
        urm_test = URM(X_test)

        urm = urm_train


        rp3b = RP3betaRecommender(urm.getCSR())
        rp3b.fit(**cf_parameters)


        cumulative_precision[i], cumulative_recall[i], cumulative_MAP[i] = evaluate_algorithm(urm_test, rp3b)


    cumulative_precision = np.array(cumulative_precision)
    cumulative_recall = np.array(cumulative_recall)
    cumulative_MAP = np.array(cumulative_MAP)

    print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}"
              .format(cumulative_precision.mean(), cumulative_recall.mean(), cumulative_MAP.mean()))

    logFile.write("Test case: {}, Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}\n".format(cf_parameters,
                                                                                                  cumulative_precision.mean(),
                                                                                                  cumulative_recall.mean(),
                                                                                                  cumulative_MAP.mean()))
    logFile.flush()




logFile = open("RP3Beta_Tuning.txt", "a")
crossValidate(logFile)