import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from PopulationRecommender.NewPopulationHybrid import NewPopulationHybrid, NewHybrid
from PopulationRecommender.PopulationSplit import population_train_split, population_split
from RecKit.ICM import ICM
from RecKit.URM import URM
from RecKit.evaluation_tools import evaluate_algorithm
from RecKit.generate_output import generate_output
from tmscarl.Builder import Builder
from tmscarl.Evaluator import Evaluator

"""              LOAD DATA           """
interactionsCsv = pd.read_csv("../input/train.csv")
targetList = pd.read_csv("../input/target_playlists.csv").iloc[:, 0]
tracksCsv = pd.read_csv("../input/tracks.csv")

slimLogFile = open("SLIM_BPR_Cython.txt", "a")

submission = True

"""        Population Splitting      """

icm_1 = ICM(tracksCsv, col="artist")
icm_2 = ICM(tracksCsv, col="album")

if submission:
    urm_train = URM(interactionsCsv)
    urm_train_t = URM(interactionsCsv[['track_id', 'playlist_id']], transposed=True)
else:
    b = Builder()
    evaluator = Evaluator()
    evaluator.split()

    playlist = []
    tracks = []
    testCSV_partial = evaluator.test_df

    for index, row in testCSV_partial.iterrows():
        for elem in row[1]:
            playlist.append(row[0])
            tracks.append(elem)

    testCSV = pd.DataFrame({'playlist_id': playlist, 'track_id': tracks})

    df_all = interactionsCsv.merge(testCSV.drop_duplicates(), on=['playlist_id', 'track_id'],
                                   how='left', indicator=True)

    trainCSV = df_all[df_all['_merge'] == 'left_only'].drop(columns=['_merge'])
    print("Shapes", trainCSV.shape, testCSV.shape)
    urm_train = URM(trainCSV)
    urm_test = URM(testCSV)
    urm_train_t = URM(trainCSV[['track_id', 'playlist_id']], transposed=True)

enabled_groups = {'2'}

"""              Params              """
""" Select only one group and alg. for tuning,
select both groups to compare MAP to the alg.
with no population split"""

enable_dict = {'enabled_groups': enabled_groups,
               'enableCBI': True, 'enableRP3B': True, 'enableCBF': True, 'enableCBU': True, 'enableSLIM': True, 'enableSVD': True}


"""             Build URM            """

matrices = {'URM': urm_train, 'URM_T': urm_train_t, 'ICM_1': icm_1, 'ICM_2': icm_2}

"""             Algorithm            """

# Last 10 from those >= 20
# CBF = Precision = 0.0379, Recall = 0.0379, MAP = 0.017076
# CBU = Precision = 0.0991, Recall = 0.0991, MAP = 0.047982
# RP3B =  Precision = 0.1033, Recall = 0.1033, MAP = 0.049679
# SLIM = Precision = 0.1030, Recall = 0.1030, MAP = 0.049577

# 10 Random from those >= 20
# CBF  Precision = 0.0789, Recall = 0.0789, MAP = 0.042482
# User Precision = 0.1684, Recall = 0.1684, MAP = 0.098375
# RP3B Precision = 0.1808, Recall = 0.1808, MAP = 0.105723
# SLIM Precision = 0.1818, Recall = 0.1818, MAP = 0.104835

# 10 only from targets from those >= 20

# CBF  Precision = 0.0748, Recall = 0.0748, MAP = 0.038180
# User Precision = 0.1662, Recall = 0.1662, MAP = 0.097595
# RP3B Precision = 0.1791, Recall = 0.1791, MAP = 0.103893
# SLIM Precision = 0.1821, Recall = 0.1821, MAP = 0.106419

# 20% from targets from those >= 20
# CBF   Precision = 0.0524, Recall = 0.0833, MAP = 0.043296
# User  Precision = 0.1217, Recall = 0.1814, MAP = 0.101109
# RP3B  Precision = 0.1305, Recall = 0.1965, MAP = 0.109342
# SLIM  Precision = 0.1314, Recall = 0.1993, MAP = 0.109529

# SVD 350 Precision = 0.1272, Recall = 0.1932, MAP = 0.103667

# Hybrid no SVD       Precision = 0.1387, Recall = 0.2090, MAP = 0.118719
# Hybrid no SVD       Precision = 0.1361, Recall = 0.2059, MAP = 0.116252

#
# Hybrid 225 SVD      Precision = 0.1413, Recall = 0.2129, MAP = 0.121282
# Hybrid 225 SVD 0.25 Precision = 0.1416, Recall = 0.2132, MAP = 0.121558
# Hybrid 225 SVD 0.3  Precision = 0.1416, Recall = 0.2133, MAP = 0.121762
# Hybrid 350 SVD 0.3  Precision = 0.1409, Recall = 0.2129, MAP = 0.120633


group2_param = {'cbi_param_dict': {'k': 100, 'h': 20, 'mode': 'item'},
                'cbu_param_dict': {'k': 150, 'h': 10, 'mode': 'user'},  # 225 0
                'cbf_param_dict': {'k': 15, 'shrinkage': 10, 'ar_weight': 2, 'al_weight': 1},

                'slim_param_dict': {'epochs': 15, 'validation_every_n': 1, 'logFile': slimLogFile, 'batch_size': 5,
                                    'topK': 120, 'sgd_mode': "adagrad", 'learning_rate': 0.05},

                'rp3b_param_dict': {'topK': 60, 'alpha': 0.7, 'beta': 0.2, 'normalize_similarity': True,
                                    'implicit': True, 'norm': 'l2'},

                'svd_param_dict': {'num_factors':300, 'reg':0.015, 'iters':15, 'scaling':'log', 'alpha':40,
                                    'epsilon':1.0, 'init_mean':0.0, 'init_std':0.1},

                'weight_dict': {'cbi_weight': 0.125, 'cbu_weight': 0.23, 'cbf_weight': 0.24, 'rp3b_weight': 0.35,
                                'slim_weight': 0.325, 'svd_weight': 0.3},
                'loadSLIM': False, 'slimPath': 'slim_G2.pickle', 'normalizeSLIM': 'l2', 'CBFNorm': None
                }

nphybrid = NewHybrid(matrices, group2_param, enable_dict)

if submission:
    print("Preparing submission")
    recommended_items = nphybrid.m_recommend(targetList, nRec=10)
    generate_output(targetList, recommended_items, path="submission.csv")

else:
    cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, nphybrid)
    print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}"
          .format(cumulative_precision, cumulative_recall, cumulative_MAP))

    """             Print to File        """
    logFile = open("Tuning.txt", "a")
    logFile.write("Test group {}, params: {}, Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}\n"
                  .format(enable_dict, group2_param, cumulative_precision.mean(), cumulative_recall.mean(),
                          cumulative_MAP.mean()))

    logFile.flush()

""" 
        weight_dict = {'cbi_weight': 0.125, 'cbu_weight': 0.23, 'cbf_weight': 0.24, 'rp3b_weight': 0.35, 'slim_weight': 0.325, 'svd_weight': 0.2}
        225  0
            Precision = 0.1354, Recall = 0.1782, MAP = 0.106595
        130  10
        Precision = 0.1340, Recall = 0.1765, MAP = 0.106276

group1_param = {'cbi_param_dict' : {'k': 150, 'h': 20, 'mode': 'item'},
                'cbu_param_dict' : {'k': 290, 'h': 2, 'mode': 'user'},
                'cbf_param_dict' : {'k': 100, 'shrinkage': 0, 'ar_weight':1.75, 'al_weight':1},

                'slim_param_dict': {'epochs':25, 'validation_every_n':100, 'logFile': slimLogFile, 'batch_size':5,
                                    'topK':500, 'sgd_mode':"adagrad", 'learning_rate':0.03},

                'rp3b_param_dict': {'topK': 80, 'alpha': 0.7, 'beta': 0.3, 'normalize_similarity': True,
                                    'implicit': True, 'norm': 'l1'},

                'weight_dict'    : {'cbi_weight': 0.15, 'cbu_weight': 0.2, 'cbf_weight': 0.25, 'rp3b_weight': 0.325, 'slim_weight':0.325},
                'loadSLIM': False, 'slimPath': 'slim_G1.pickle'
                }

                0.1270
"""

"""
    FOR TUNING AFTER BUILDING MODEL


for i in range(70):
    weight_dict = {'cbi_weight': round(random.uniform(0.0, 0.5),3), 'cbu_weight': round(random.uniform(0.0, 0.5),3),
                    'cbf_weight': round(random.uniform(0.0, 0.5),3), 'rp3b_weight': round(random.uniform(0.0, 0.5),3),
                    'slim_weight': round(random.uniform(0.0, 0.5),3)}
    nphybrid.setWeights(3, weight_dict)
    cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, nphybrid)
    print("Recommender, performance is: Precision = {:.5f}, Recall = {:.5f}, MAP = {:.6f}"
          .format(cumulative_precision, cumulative_recall, cumulative_MAP))
    logFile = open("Tuning.txt", "a")
    logFile.write("Built model tuning, weights= {} Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f} ".format(weight_dict, cumulative_precision, cumulative_recall, cumulative_MAP))
    logFile.flush()


    OR

    weight_dict= {'cbi_weight': 0.125, 'cbu_weight': 0.23, 'cbf_weight': 0.24, 'rp3b_weight':0.35, 'slim_weight':0.325}
    nphybrid.setWeights(3, weight_dict)
    cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, nphybrid)
    print("Recommender, performance is: Precision = {:.5f}, Recall = {:.5f}, MAP = {:.6f}"
          .format(cumulative_precision, cumulative_recall, cumulative_MAP))
    logFile = open("Tuning.txt", "a")
    logFile.write("Built model tuning, weights= {} Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f} ".format(weight_dict, cumulative_precision, cumulative_recall, cumulative_MAP))
    logFile.flush()


    weight_dict = {'cbi_weight': 0.125, 'cbu_weight': 0.23, 'cbf_weight': 0.24, 'rp3b_weight':0.35, 'slim_weight':0.325}
"""

