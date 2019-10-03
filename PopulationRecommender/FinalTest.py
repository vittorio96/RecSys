import pandas as pd
from tqdm import tqdm

from PopulationRecommender.NewPopulationHybrid import NewHybrid, NewPopulationHybrid
from RecKit.ICM import ICM
from RecKit.URM import URM
from RecKit.evaluation_tools import evaluate_algorithm
from RecKit.perfect_split import perfect_split
from RecKit.generate_output import generate_output
from PopulationRecommender.PopulationSplit import population_train_split, population_split

"""              LOAD FILES           """

interactionsCsv = pd.read_csv("../input/train.csv")
targetList = pd.read_csv("../input/target_playlists.csv").iloc[:, 0]
tracksCsv = pd.read_csv("../input/tracks.csv")

slimLogFile = open("SLIM_BPR_Cython.txt", "a")

"""              PARAMS              """

submission = True
values = [14, 35]
#enabled_groups = {'1','2','3'}
enabled_groups = {'1','2','3'}
enable_dict = {'enabled_groups': enabled_groups,
               'enableCBI': True, 'enableRP3B': True, 'enableCBF': True,
               'enableCBU': True, 'enableSLIM': True, 'enableSVD': True}

"""        Population Splitting      """

train_group1, train_group2, train_group3 = population_train_split(interactionsCsv, method=('threshold', values))
group1, group2, group3 = population_split(interactionsCsv, method=('threshold', values))

if '1' in enabled_groups:
    X_train_1, X_test_1 = perfect_split(train_group1, targetList, test_size=0.2, method='20p')
else:
    X_train_1, X_test_1 = train_group1, None

if '2' in enabled_groups:
    X_train_2, X_test_2 = perfect_split(train_group2, targetList, test_size=0.2, method='20p')
else:
    X_train_2, X_test_2 = train_group2, None

if '3' in enabled_groups:
    X_train_3, X_test_3 = perfect_split(train_group3, targetList, test_size=0.2,  method='20p')
else:
    X_train_3, X_test_3 = train_group3, None

X_train = pd.concat([X_train_1, X_train_2, X_train_3])
X_test = pd.concat([X_test_1, X_test_2, X_test_3])

# G1 (1185556, 2) (26235, 2)
# G2 (1119337, 2) (92454, 2)        Precision = 0.0991, Recall = 0.2313, MAP = 0.123951
# G3 (1107769, 2) (104022, 2)       Precision = 0.1809, Recall = 0.1936, MAP = 0.119263
# Tot (989080, 2) (222711, 2)
"""        Build Matrices      """

icm_1 = ICM(tracksCsv, col="artist")
icm_2 = ICM(tracksCsv, col="album")

if submission:
    urm_train = URM(interactionsCsv)
    urm_train_t = URM(interactionsCsv[['track_id', 'playlist_id']], transposed=True)
else:
    urm_train = URM(X_train)
    urm_train_t = URM(X_train[['track_id', 'playlist_id']], transposed=True)
    #urm_test = URM(X_test)
    urm_test = URM(X_test_1)

matrices = {'URM': urm_train, 'URM_T': urm_train_t,
            'ICM_1': icm_1,   'ICM_2': icm_2}

"""             Algorithm            """
if submission:
    slimPathG1 = 'slim_G1_FS.pickle'; svdPathG1 = 'svd_G1_2_3_FS.pickle'#same svd als for all groups
    slimPathG2 = 'slim_G2_FS.pickle'; svdPathG2 = 'svd_G1_2_3_FS.pickle'
    slimPathG3 = 'slim_G3_FS.pickle'; svdPathG3 = 'svd_G1_2_3_FS.pickle'
else:
    slimPathG1 = 'slim_G1_FT.pickle'; svdPathG1 = 'svd_G1_2_3_FT.pickle'
    slimPathG2 = 'slim_G2_FT.pickle'; svdPathG2 = 'svd_G1_2_3_FT.pickle'
    slimPathG3 = 'slim_G3_FT.pickle'; svdPathG3 = 'svd_G1_2_3_FT.pickle'

group1_param = {'cbi_param_dict' : {'k': 225, 'h': 20, 'mode': 'item'},
                'cbu_param_dict' : {'k': 290, 'h': 2, 'mode': 'user'},
                'cbf_param_dict' : {'k': 100, 'shrinkage': 0, 'ar_weight':1.75, 'al_weight':1},

                'slim_param_dict': {'epochs':25, 'validation_every_n':100, 'logFile': slimLogFile, 'batch_size':5,
                                    'topK':500, 'sgd_mode':"adagrad", 'learning_rate':0.03},

                'rp3b_param_dict': {'topK': 80, 'alpha': 0.7, 'beta': 0.3, 'normalize_similarity': True,
                                    'implicit': True, 'norm': 'l2'},

                'svd_param_dict': {'num_factors':10, 'reg':0.015, 'iters':18, 'scaling':'log', 'alpha':40,
                                    'epsilon':1.0, 'init_mean':0.0, 'init_std':0.1},

                'weight_dict'    : {'cbi_weight': 0.22, 'cbu_weight': 0.28, 'cbf_weight': 0.3, 'rp3b_weight': 0.172, 'slim_weight': 0.431, 'svd_weight': 0.41} ,
                #'weight_dict' : {'cbi_weight': 0.3, 'cbu_weight': 1.15,'cbf_weight': 0.45, 'rp3b_weight': 0.3,'slim_weight': 0.35, 'svd_weight': 0.35},

                'loadSLIM': True, 'slimPath': slimPathG1, 'loadSVD': True, 'svdPath': svdPathG1, 'normalizeSLIM': 'l2', 'CBFNorm' : 'l2'
                }

group2_param = {'cbi_param_dict' : {'k': 100, 'h': 20, 'mode': 'item'},
                'cbu_param_dict' : {'k': 150, 'h': 10, 'mode': 'user'},
                'cbf_param_dict' : {'k': 20, 'shrinkage': 10, 'ar_weight':2, 'al_weight':1},

                'slim_param_dict': {'epochs':15, 'validation_every_n':1, 'logFile':slimLogFile, 'batch_size':5,
                                    'topK':120, 'sgd_mode':"adagrad", 'learning_rate':0.05},

                'rp3b_param_dict': {'topK': 60, 'alpha': 1, 'beta': 0.3, 'normalize_similarity': True,
                                    'implicit': True, 'norm': 'l2'},

                'svd_param_dict': {'num_factors':340, 'reg':0.015, 'iters':18, 'scaling':'log', 'alpha':40,
                                    'epsilon':1.0, 'init_mean':0.0, 'init_std':0.1},

                'weight_dict'    : {'cbi_weight': 0.1, 'cbu_weight': 0.38, 'cbf_weight': 0.28, 'rp3b_weight': 0.375, 'slim_weight': 0.3, 'svd_weight': 0.42} ,
                'loadSLIM': True, 'slimPath': slimPathG2, 'loadSVD': True, 'svdPath': svdPathG2, 'normalizeSLIM': 'l2', 'CBFNorm' : 'l2'
                }

group3_param = {'cbi_param_dict' : {'k': 100, 'h': 20, 'mode': 'item'},
                'cbu_param_dict' : {'k': 200, 'h': 5, 'mode': 'user'},
                'cbf_param_dict' : {'k': 100, 'shrinkage': 10, 'ar_weight':2, 'al_weight':1},

                'slim_param_dict': {'epochs':15, 'validation_every_n':1, 'logFile':slimLogFile, 'batch_size':5,
                                    'topK':200, 'sgd_mode':"adagrad", 'learning_rate':0.05},

                'rp3b_param_dict': {'topK': 50, 'alpha': 1, 'beta': 0.3, 'normalize_similarity': True,
                                    'implicit': True, 'norm': 'l2'},

                'svd_param_dict': {'num_factors':300, 'reg':0.015, 'iters':18, 'scaling':'log', 'alpha':40,
                                                    'epsilon':1.0, 'init_mean':0.0, 'init_std':0.1},

                'weight_dict'    : {'cbi_weight': 0.1, 'cbu_weight': 0.36, 'cbf_weight': 0.28, 'rp3b_weight': 0.375, 'slim_weight': 0.3, 'svd_weight': 0.42},

                'loadSLIM': True, 'slimPath': slimPathG3, 'loadSVD': True, 'svdPath': svdPathG3,
                'normalizeSLIM': 'l2', 'CBFNorm' : 'l2'
                }


groups = [group1, group2, group3]
group_params = [group1_param, group2_param, group3_param]

nphybrid = NewPopulationHybrid(matrices, groups, group_params, enable_dict)


if submission:
    print("Preparing submission")
    recommended_items = nphybrid.m_recommend(targetList, nRec=10)
    generate_output(targetList, recommended_items, path="submission.csv")

else:
    cumulative_precision, cumulative_recall, cumulative_MAP = tqdm(evaluate_algorithm(urm_test, nphybrid))
    print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}"
          .format(cumulative_precision, cumulative_recall, cumulative_MAP))

    """             Print to File        """
    logFile = open("Tuning_vittorio.txt", "a")
    logFile.write("Test group {}, params: {}, Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}\n"
                  .format(enable_dict, group2_param, cumulative_precision.mean(), cumulative_recall.mean(),
                          cumulative_MAP.mean()))

    logFile.flush()



"""
    FOR TUNING AFTER BUILDING MODEL

for i in range(70):
    weight_dict = {'cbi_weight': 0.5, 'cbu_weight': 1,
                    'cbf_weight': round(np.random.uniform(0.0, 0.5), 3), 'rp3b_weight': 0,
                    'slim_weight': round(np.random.uniform(0.0, 0.5), 3)}
    nphybrid.setWeights(1, weight_dict)
    cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, nphybrid)
    print("Recommender, performance is: Precision = {:.5f}, Recall = {:.5f}, MAP = {:.6f}"
          .format(cumulative_precision, cumulative_recall, cumulative_MAP))
    logFile = open("Tuning_vittorio.txt", "a")
    logFile.write("Built model tuning, weights= {} Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f} ".format(weight_dict, cumulative_precision, cumulative_recall, cumulative_MAP))
    logFile.flush()

for slim in [0.2, 0.3, 0.35]:
    weight_dict = {'cbi_weight': 0.3, 'cbu_weight': 1.15,
                   'cbf_weight': 0.45, 'rp3b_weight': 0.3,
                   'slim_weight': 0.35, 'svd_weight': 0.35}
    nphybrid.setWeights(1, weight_dict)
    cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, nphybrid)
    print("Recommender, performance is: Precision = {:.5f}, Recall = {:.5f}, MAP = {:.6f}"
          .format(cumulative_precision, cumulative_recall, cumulative_MAP))
    logFile = open("Tuning_vittorio.txt", "a")
    logFile.write("Built model tuning, weights= {} Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f} \n".format(weight_dict, cumulative_precision,cumulative_recall,cumulative_MAP))
    logFile.flush()


    OR

    weight_dict= {'cbi_weight': 0.05, 'cbu_weight': 0.354, 'cbf_weight': 0.225, 'rp3b_weight': 0.375, 'slim_weight': 0.307, 'svd_weight': 0.3}
    nphybrid.setWeights(3, weight_dict)
    cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, nphybrid)
    print("Recommender, performance is: Precision = {:.5f}, Recall = {:.5f}, MAP = {:.6f}"
          .format(cumulative_precision, cumulative_recall, cumulative_MAP))
    logFile = open("Tuning_vittorio.txt", "a")
    logFile.write("Built model tuning, weights= {} Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f} ".format(weight_dict, cumulative_precision, cumulative_recall, cumulative_MAP))
    logFile.flush()


    weight_dict = {'cbi_weight': 0.125, 'cbu_weight': 0.23, 'cbf_weight': 0.24, 'rp3b_weight':0.35, 'slim_weight':0.325}
"""

