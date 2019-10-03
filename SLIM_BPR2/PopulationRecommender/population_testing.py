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
values = [14, 35]
random_seed = 12
enabled_groups = {'1'}
#enabled_groups = {'1','2','3'}
train_group1, train_group2, train_group3 = population_train_split(interactionsCsv, method=('threshold', values))
group1, group2, group3 = population_split(interactionsCsv, method=('threshold', values))
print("G1 In common with target playlists {}".format(len(targetList[np.isin(targetList, group1)])))
print("G2 In common with target playlists {}".format(len(targetList[np.isin(targetList, group2)])))
print("G3 In common with target playlists {}".format(len(targetList[np.isin(targetList, group3)])))
if '1' in enabled_groups:
    X_train1, X_test1 = train_test_split(train_group1, test_size=0.2, random_state=random_seed)
else:
    X_train1, X_test1 = train_test_split(train_group1, test_size=0, random_state=random_seed)

if '2' in enabled_groups:
    X_train2, X_test2 = train_test_split(train_group2, test_size=0.2, random_state=random_seed)
else:
    X_train2, X_test2 = train_test_split(train_group2, test_size=0, random_state=random_seed)

if '3' in enabled_groups:
    X_train3, X_test3 = train_test_split(train_group3, test_size=0.2, random_state=random_seed)
else:
    X_train3, X_test3 = train_test_split(train_group3, test_size=0, random_state=random_seed)

X_train = pd.concat([X_train1, X_train2, X_train3])
X_test = pd.concat([X_test1, X_test2, X_test3])

"""              Params              """
""" Select only one group and alg. for tuning, 
select both groups to compare MAP to the alg.
with no population split"""

enable_dict = {'enabled_groups': enabled_groups,
               'enableCBI': False, 'enableRP3B': False, 'enableCBF': False, 'enableCBU': True, 'enableSLIM': False}
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

group1_param = {'cbi_param_dict' : {'k': 225, 'h': 20, 'mode': 'item'},
                'cbu_param_dict' : {'k': 290, 'h': 2, 'mode': 'user'},
                'cbf_param_dict' : {'k': 100, 'shrinkage': 0, 'ar_weight':1.75, 'al_weight':1},

                'slim_param_dict': {'epochs':25, 'validation_every_n':100, 'logFile': slimLogFile, 'batch_size':5,
                                    'topK':500, 'sgd_mode':"adagrad", 'learning_rate':0.03},

                'rp3b_param_dict': {'topK': 80, 'alpha': 0.7, 'beta': 0.3, 'normalize_similarity': True,
                                    'implicit': True, 'norm': 'l2'},

                'weight_dict'    : {'cbi_weight': 0.09, 'cbu_weight': 0.2, 'cbf_weight': 0.25, 'rp3b_weight': 0.345, 'slim_weight':0.325},
                'loadSLIM': False, 'slimPath': 'slim_G1.pickle', 'normalizeSLIM': 'l2', 'CBFNorm' : None
                }

group2_param = {'cbi_param_dict' : {'k': 100, 'h': 20, 'mode': 'item'},
                'cbu_param_dict' : {'k': 150, 'h': 10, 'mode': 'user'}, #225 0
                'cbf_param_dict' : {'k': 30, 'shrinkage': 10, 'ar_weight':2, 'al_weight':1},

                'slim_param_dict': {'epochs':15, 'validation_every_n':1, 'logFile':slimLogFile, 'batch_size':5,
                                    'topK':120, 'sgd_mode':"adagrad", 'learning_rate':0.05},

                'rp3b_param_dict': {'topK': 60, 'alpha': 1, 'beta': 0.3, 'normalize_similarity': True,
                                    'implicit': True, 'norm': 'l2'},

                'weight_dict'    : {'cbi_weight': 0.125, 'cbu_weight': 0.23, 'cbf_weight': 0.24, 'rp3b_weight':0.35, 'slim_weight':0.325},
                'loadSLIM': False, 'slimPath': 'slim_G2.pickle', 'normalizeSLIM': 'l2', 'CBFNorm' : None
                }

group3_param = {'cbi_param_dict' : {'k': 100, 'h': 20, 'mode': 'item'},
                'cbu_param_dict' : {'k': 150, 'h': 5, 'mode': 'user'},
                'cbf_param_dict' : {'k': 100, 'shrinkage': 10, 'ar_weight':2, 'al_weight':1},

                'slim_param_dict': {'epochs':15, 'validation_every_n':1, 'logFile':slimLogFile, 'batch_size':5,
                                    'topK':200, 'sgd_mode':"adagrad", 'learning_rate':0.05},

                'rp3b_param_dict': {'topK': 50, 'alpha': 1, 'beta': 0.3, 'normalize_similarity': True,
                                    'implicit': True, 'norm': 'l2'},

                'weight_dict'    : {'cbi_weight': 0.012, 'cbu_weight': 0.354, 'cbf_weight': 0.225, 'rp3b_weight': 0.375, 'slim_weight': 0.307},
                'loadSLIM': False, 'slimPath': 'slim_G3.pickle', 'normalizeSLIM': 'l2', 'CBFNorm' : None
                }

group_param = [group1_param, group2_param, group3_param]
nphybrid = NewPopulationHybrid(matrices, group1, group2, group3, group1_param, group2_param, group3_param, enable_dict)

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
    
    
for i in range(15):    
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

