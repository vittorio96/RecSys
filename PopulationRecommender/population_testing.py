import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from PopulationRecommender.NewPopulationHybrid import NewPopulationHybrid
from PopulationRecommender.PopulationSplit import population_train_split, population_split
from RecKit.ICM import ICM
from RecKit.URM import URM
from RecKit.evaluation_tools import evaluate_algorithm
from RecKit.generate_output import generate_output

"""              LOAD DATA           """
interactionsCsv = pd.read_csv("input/train.csv")
targetList = pd.read_csv("input/target_playlists.csv").iloc[:,0]
tracksCsv = pd.read_csv("input/tracks.csv")

slimLogFile = open("SLIM_BPR_Cython.txt", "a")

"""        Population Splitting      """
value = 14
train_group1, train_group2 = population_train_split(interactionsCsv, method=('threshold', value))
group1, group2 = population_split(interactionsCsv, method=('threshold', value))
print("In common with target playlists {}".format(len(targetList[np.isin(targetList, group1)])))

enabled_groups = {'1','1'}

if '1' in enabled_groups:
    X_train1, X_test1 = train_test_split(train_group1, test_size=0.25, random_state=12)

if '1' not in enabled_groups:
    X_train1, X_test1 = train_test_split(train_group1, test_size=0, random_state=12)

if '2' in enabled_groups:
    X_train2, X_test2 = train_test_split(train_group2, test_size=0.25, random_state=12)

if '2' not in enabled_groups:
    X_train2, X_test2 = train_test_split(train_group2, test_size=0, random_state=12)

X_train = pd.concat([X_train1, X_train2])
X_test = pd.concat([X_test1, X_test2])

"""              Params              """
""" Select only one group and alg. for tuning, 
select both groups to compare MAP to the alg.
with no population split"""

enable_dict = {'enabled_groups': enabled_groups,
               'enableCBI': False, 'enableRP3B': False, 'enableCBF': True, 'enableCBU': False, 'enableSLIM': False}
submission = False

"""             Build URM            """
if submission :
    X_train = interactionsCsv

urm_train = URM(X_train)
urm_test = URM(X_test)
icm_1 = ICM(tracksCsv, col="artist")
icm_2 = ICM(tracksCsv, col="album")

X_train_t = X_train[['track_id', 'playlist_id']]
urm_train_t = URM(X_train_t, transposed=True)

matrices = {'URM': urm_train, 'URM_T': urm_train_t, 'ICM_1': icm_1, 'ICM_2': icm_2}

"""             Algorithm            """

"""G1 0.121 G2 0.116 -> Server 0.09042"""
"""G1 0.125 G2 0.117 -> Server 0.09134"""
group1_param = {'cbi_param_dict' : {'k': 150, 'h': 20, 'mode': 'item'},
                'cbu_param_dict' : {'k': 150, 'h': 20, 'mode': 'user'},
                'cbf_param_dict' : {'k': 50, 'shrinkage': 10, 'ar_weight':1.75, 'al_weight':1},

                'slim_param_dict': {'epochs':20, 'validation_every_n':100, 'logFile': slimLogFile, 'batch_size':5,
                                    'topK':250, 'sgd_mode':"adagrad", 'learning_rate':0.075},

                'rp3b_param_dict': {'topK': 100, 'alpha': 0.7, 'beta': 0.3, 'normalize_similarity': True,
                                    'implicit': True, 'norm': 'l1'},

                'weight_dict'    : {'cbi_weight': 0.15, 'cbu_weight': 0.2, 'cbf_weight': 0.25, 'rp3b_weight': 0.325, 'slim_weight':0.325},
                'loadSLIM': True, 'slimPath': 'slim_G1.pickle'
                }

group2_param = {'cbi_param_dict' : {'k': 125, 'h': 10, 'mode': 'item'},
                'cbu_param_dict' : {'k': 200, 'h': 4, 'mode': 'user'},
                'cbf_param_dict' : {'k': 50, 'shrinkage': 10, 'ar_weight':2, 'al_weight':1},

                'slim_param_dict': {'epochs':20, 'validation_every_n':1, 'logFile':slimLogFile, 'batch_size':5,
                                    'topK':200, 'sgd_mode':"adagrad", 'learning_rate':0.075},

                'rp3b_param_dict': {'topK': 100, 'alpha': 0.7, 'beta': 0.3, 'normalize_similarity': True,
                                    'implicit': True, 'norm': 'l1'},

                'weight_dict'    : {'cbi_weight': 0, 'cbu_weight': 0.325, 'cbf_weight': 0.2, 'rp3b_weight': 0.225, 'slim_weight': 0.325},
                'loadSLIM': True, 'slimPath': 'slim_G2.pickle'
                }

group_param = [group1_param, group2_param]

nphybrid = NewPopulationHybrid(matrices, group1, group2, group1_param, group2_param, enable_dict)

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
                    .format(enable_dict, group_param, cumulative_precision.mean(), cumulative_recall.mean(),
                    cumulative_MAP.mean()))

    logFile.flush()


""" 
    FOR TUNING AFTER BUILDING MODEL
for x in [0.3, 0.5, 0.4, 0.325, 0.375, 0.2]:    
    weight_dict = {'cbi_weight': 0.15, 'cbu_weight': 0.2, 'cbf_weight': 0.25, 'rp3b_weight': 0.325, 'slim_weight':0.325}
    nphybrid.setWeights(1, weight_dict)
    cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, nphybrid)
    print("Recommender, performance is: Precision = {:.5f}, Recall = {:.5f}, MAP = {:.6f}"
          .format(cumulative_precision, cumulative_recall, cumulative_MAP))
    logFile = open("Tuning.txt", "a")
    logFile.write("Built model tuning, weights= {} Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f} ".format(weight_dict, cumulative_precision, cumulative_recall, cumulative_MAP))
    logFile.flush()
    
    
    weight_dict = {'cbi_weight': 0.125, 'cbu_weight': 0.23, 'cbf_weight': 0.24, 'rp3b_weight':0.35, 'slim_weight':0.325}
"""

