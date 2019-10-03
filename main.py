import pandas as pd

from BasicRecommenders.CollaborativeFiltering import CollaborativeFiltering
from BasicRecommenders.ContentBasedFiltering import ContentBasedFiltering
from BasicRecommenders.RevisedCBF import RevisedCBF
from BasicRecommenders.SVDRecommender import SVDRecommender
from CFandCBF.FW_Similarity.Cython.CFW_D_Similarity_Cython import CFW_D_Similarity_Cython
from BasicRecommenders.RP3beta import RP3betaRecommender
from HybridRecommenders.ListHybridRecommender import ListHybridRecommender
from HybridRecommenders.PupulationHybrid import PopulationHybrid
from HybridRecommenders.UserItemHybridRecommender import UserItemHybridRecommender
from BasicRecommenders.LightFMRecommender import LightFMRecommender
from BasicRecommenders.P3alpha import P3alpha
from MatrixFactorization.MatrixFactorization_BPR_Theano import MatrixFactorization_BPR_Theano
from MatrixFactorization.MatrixFactorization_RMSE import BPRMF, AsySVD, IALS_numpy
from MatrixFactorization.PureSVD import PureSVDRecommender
from RecKit.ICM import ICM
from RecKit.URM import URM
from RecKit.evaluation_tools import evaluate_algorithm
from RecKit.generate_output import generate_output
from sklearn.model_selection import train_test_split
from HybridRecommenders.ItemItemHybridRecommender import IIHybridRecommender
from RecKit.getURMThreshold import getURMThreshold
from SLIM_BPR.Slim_Elastic_Net import Slim_Elastic_Net
from SLIM_BPR2.Cython.SLIM_BPR_Cython import SLIM_BPR_Cython
from BasicRecommenders.XGBoostRecommender import XGBoostRecommender

Kaggle=False

if Kaggle==True:
    interactionsCsv = pd.read_csv("../input/train.csv")
    targetList = pd.read_csv("../input/target_playlists.csv").iloc[:,0]
    tracksCsv = pd.read_csv("../input/tracks.csv")
else:
    interactionsCsv = pd.read_csv("input/train.csv")
    targetList = pd.read_csv("input/target_playlists.csv").iloc[:,0]
    tracksCsv = pd.read_csv("input/tracks.csv")

print(interactionsCsv.describe())
icm = ICM(tracksCsv, col="artist")
icm2 = ICM(tracksCsv, col="album")
urm_full = URM(interactionsCsv)
X_train, X_test = train_test_split(interactionsCsv, test_size =0.05, random_state=17)

urm_train = URM(X_train)
urm_test = URM(X_test)

# Transposed matrix
X_train_t = X_train[['track_id', 'playlist_id']]
X_test_t = X_train_t[['track_id', 'playlist_id']]
urm_full_t = URM(interactionsCsv[['track_id', 'playlist_id']], transposed=True)
urm_test_t = URM(X_test_t, transposed=True)
urm_train_t = URM(X_train_t, transposed=True)

"""
    RUNNING SCRIPT PARAMETERS
"""
submission = False; htype = "als"



if submission == True:
    urm = urm_full
    urm_t = urm_full_t
else:
    urm = urm_train
    urm_t = urm_train_t


if htype == "pophyb":


    group_1_2_TH = getURMThreshold(urm, 20)

    group_1_params = {'user_weight': 0.4, 'item_weight': 0.4, 'cbf_weight': 0.14, 'cbf2_weight': 0.1,
                      'slim_weight': 0.14, 'svd_weight': 0.11}

    group_2_params = {'user_weight': 0.23, 'item_weight': 0.325, 'cbf_weight': 0.15, 'cbf2_weight': 0.10,
                      'slim_weight': 0.335, 'svd_weight': 0.13}

    param_dict = {'n_groups': 2, 'group_1_params': group_1_params, 'group_2_params': group_2_params,
                  'group_1_2_TH': group_1_2_TH}

    phy = PopulationHybrid(urm, urm_t, icm, icm2, enable_dict, param_dict, urm_test=urm_test)
    if submission:
        recommended_items = phy.m_recommend(targetList, nRec=10)
        generate_output(targetList, recommended_items)
    else:
        print("Evaluating")
        cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, phy)
        print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.5f}".format(cumulative_precision, cumulative_recall, cumulative_MAP))

elif htype == "uii":

    print("Fitting hybrid recommender")

    enable_dict = {'enableSVD'  : True, 'enableSLIM' : True,
                   'enableCBF2' : True,  'enableP3A'  : False,
                   'enableLFM'  : False, 'enableRP3B' : True}


    hr = UserItemHybridRecommender(urm, urm_t, icm, icm2, enable_dict, urm_test=urm_test)
    print("Mixing predictions")

    """
    Recommender, performance is: 
    
    Hybrid      0.4, 0.4,  0.14, 0.10, 0.13, 0.13
                Precision = 0.0265, Recall = 0.2266, MAP = 0.10524 -> 0.09091 Server
             v2 Precision = 0.0270, Recall = 0.2317, MAP = 0.10661 (23) -> 0.9114 Server
     5 epoch v2 Precision = 0.0269, Recall = 0.2301, MAP = 0.10549 (23) -> 
             v2 Precision = 0.0267, Recall = 0.2315, MAP = 0.10540 (17) -> 0.9114 Server
             
                0.4, 0.4, 0.140, 0.140, 0.110, 0.110
                Precision = 0.0267, Recall = 0.2288, MAP = 0.10515
                
                0.4, 0.4, 0.145, 0.135, 0.110, 0.110
                Precision = 0.0267, Recall = 0.2286, MAP = 0.10521
                
                0.455, 0.345, 0.145, 0.135, 0.110, 0.110
                Precision = 0.0268, Recall = 0.2300, MAP = 0.10525 -> 0.09065 Server
                
    Hybrid_v2   0.4, 0.4, 0.14, 0.10, 0.13, 0.13 -> Local 0.10540  Server 0.09114
                0.23 0.4  0.14  0.10  0.28  0.11 -> Local 0.10637  Server 0.09171
                0.23 0.325 0.15 0.1   0.335 0.13 -> Local 0.10728  Server 0.09189  Precision = 0.0270, Recall = 0.2336
                                                    Local 0.10792  Server 0.09168  Precision = 0.0272, Recall = 0.2354               
    
    Only user   Precision = 0.0231, Recall = 0.1980, MAP = 0.09100
    Only Item   Precision = 0.0234, Recall = 0.2003, MAP = 0.09222
    CBF1        Precision = 0.0106, Recall = 0.0917, MAP = 0.04316 -> TFIDF 0.042966
    CBF2        Precision = 0.0080, Recall = 0.0710, MAP = 0.02609 -> TFIDF 0.025113
    SLIM        Precision = 0.0193, Recall = 0.1666, MAP = 0.07331
    SLIM_v2     Precision = 0.0254, Recall = 0.2176, MAP = 0.09889
    SVD         Precision = 0.0193, Recall = 0.1626, MAP = 0.06996 -> 0.06154 Server
    P3Alpha     Precision = 0.0250, Recall = 0.2150, MAP = 0.09708  
    RP3Beta     Precision = 0.0256, Recall = 0.2190, MAP = 0.10122 
    """

    "Precision = 0.0276, Recall = 0.2365, MAP = 0.11065"

    weights_dict = {'user_weight': 0.23, 'item_weight': 0.125,
                    'cbf_weight': 0.15, 'cbf2_weight': 0.1,
                    'slim_weight': 0.335, 'p3a_weight': 0.0,
                    'lfm_weight': 0.33, 'rp3b_weight': 0.355,
                    'svd_weight': 0.1}

    hr.fit(weights_dict, method='rating_weight', norm='max')

    if submission:
        recommended_items = hr.m_recommend(targetList, nRec=10)
        generate_output(targetList, recommended_items)
    else:
        print("Evaluating")
        cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, hr)
        print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.5f}".format(cumulative_precision, cumulative_recall, cumulative_MAP))

elif htype == "ii":
    hr = IIHybridRecommender(urm, icm, icm2)
    hr.fit(item_weight=0.4, cbf1_weight=0.25, cbf2_weight=0.1)
    if submission:
        recommended_items = hr.m_recommend(targetList, nRec=10)
        generate_output(targetList, recommended_items)
    else:
        cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, hr)
        print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.5f}"
              .format(cumulative_precision, cumulative_recall, cumulative_MAP))

elif htype == "icbf":
    cb = RevisedCBF(icm.getCSR(), urm.getCSR(), sparse_weights=True)
    cb.fit(topK=50, shrink=10, similarity='cosine', normalize=True, feature_weighting = "TF-IDF") # artist
    if submission:
        recommended_items = cb.m_recommend(targetList, nRec=10)
        generate_output(targetList, recommended_items)
    else:
        cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, cb)
        print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}"
                  .format(cumulative_precision, cumulative_recall, cumulative_MAP))

elif htype == "cbf":
    cbf = ContentBasedFiltering(icm2, urm, k=15, shrinkage=0)
    cbf.fit()

    if submission:
        recommended_items = cbf.m_recommend(targetList, nRec=10)
        generate_output(targetList, recommended_items)
    else:
        cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, cbf)
        print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}"
                  .format(cumulative_precision, cumulative_recall, cumulative_MAP))

elif htype == "slim":
    slim = SLIM_BPR_Cython(urm.getCSR(), recompile_cython=False, positive_threshold=0, URM_validation=urm_test.getCSR(),
                           final_model_sparse_weights=True, train_with_sparse_weights=False)
    logFile = open("SLIM_BPR_Cython.txt", "a")
    parameters={'epochs':10, 'validation_every_n':99,'logFile':logFile, 'batch_size':1, 'topK':200,
                'sgd_mode':"rmsprop", 'learning_rate':0.1, 'gamma':0.995, 'beta_1':0., 'beta_2':0.0}

    slim.fit(**parameters)
    cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, slim)
    logFile2 = open("SLIM_BPR_CythonTestCases.txt", "a")
    logFile2.write("Test case: {}, Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}\n".format(parameters,
                                                                                              cumulative_precision.mean(),
                                                                                              cumulative_recall.mean(),
                                                                                              cumulative_MAP.mean()))
    logFile2.flush()

    # with open('slim_test.pkl', 'wb') as output:
    #     pickle.dump(slim, output, pickle.HIGHEST_PROTOCOL)
    # cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, slim)
    # print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}"
    #       .format(cumulative_precision, cumulative_recall, cumulative_MAP))

elif htype == "slim_en":
    slimen = Slim_Elastic_Net(urm.getCSR())
    slimen.fit(l1_penalty=0.1, l2_penalty=0.1, positive_only=True, topK=100)
    cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, slimen)
    print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}"
          .format(cumulative_precision, cumulative_recall, cumulative_MAP))

elif htype == "svd":
    svd = SVDRecommender(urm, nf=385)
    print(type(svd.s_recommend(0)))
    # cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, svd)
    # print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}"
    #       .format(cumulative_precision, cumulative_recall, cumulative_MAP))

elif htype=="cbu":

    enable_dict = {'enableSVD': False, 'enableSLIM': True, 'enableUSER': True}

    cbu = CollaborativeFiltering()
    cbu.fit(urm_t, k=100, h=0, mode='user')
    if submission:
        recommended_items = cbu.m_recommend(targetList, nRec=10)
        generate_output(targetList, recommended_items)
    else:
        cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, cbu)
        print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}"
              .format(cumulative_precision, cumulative_recall, cumulative_MAP))

elif htype=="cbi":
    cbi = CollaborativeFiltering()
    cbi.fit(urm, k=100, h=0, mode='item')
    if submission:
        recommended_items = cbi.m_recommend(targetList, nRec=10)
        generate_output(targetList, recommended_items)
    else:
        cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, cbi)
        print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}"
              .format(cumulative_precision, cumulative_recall, cumulative_MAP))

elif htype=="lhr":
    enable_dict = {'enableSVD': True, 'enableSLIM': True, 'enableUSER': True, 'enableP3A': True}
    weights_dict = {'item_item_weight': 1.3, 'svd_weight': 0.24, 'slim_weight': 1.2, 'user_weight': 1.1,
                    'p3a_weight': 1.1}

    lhr = ListHybridRecommender(urm, urm_t, icm, icm2, enable_dict, urm_test)
    lhr.fit(weights_dict, norm="max", w_method='parab')

    if submission:
        recommended_items = lhr.m_recommend(targetList, nRec=10)
        generate_output(targetList, recommended_items)
    else:
        #lhr.fit(weights_dict, norm="max", w_method='parab')
        cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, lhr)
        print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}"
              .format(cumulative_precision, cumulative_recall, cumulative_MAP))

elif htype=="p3a":

    p3alpha = P3alpha(urm.getCSR())
    p3alpha.fit(topK=80, alpha=1, min_rating=0, implicit=True, normalize_similarity=True)
    print(type(p3alpha.s_recommend(0)))

    if submission:
        recommended_items = p3alpha.m_recommend(targetList, nRec=10)
        generate_output(targetList, recommended_items)
    else:
        cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, p3alpha)
        print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}"
              .format(cumulative_precision, cumulative_recall, cumulative_MAP))

elif htype=="cfw":

    cbi = CollaborativeFiltering()
    cbi.fit(urm, k=100, h=0, mode='item')

    fw_parameters = {'epochs': 200,
                     'learning_rate': 0.01,
                     'sgd_mode': 'adam',
                     'add_zeros_quota': 1.0,
                     'l1_reg': 0.1,
                     'l2_reg': 0.01,
                     'topK': 100,
                     'use_dropout': False,
                     'dropout_perc': 0.7,
                     'init_type': 'TF-IDF',
                     'positive_only_weights': True,
                     'normalize_similarity': True}

    cfw = CFW_D_Similarity_Cython(urm.getCSR(), icm.getCSR(), cbi.cosineSimilarityMatrix.copy())
    cfw.fit(**fw_parameters, validation_metric="MAP")

    if submission:
        recommended_items = cfw.m_recommend(targetList, nRec=10)
        generate_output(targetList, recommended_items)
    else:
        cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, cfw)
        print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}"
              .format(cumulative_precision, cumulative_recall, cumulative_MAP))

elif htype=="rp3b":
    cf_parameters = {'topK': 80,
                     'alpha': 1,
                     'beta': 0.275,
                     'normalize_similarity': True,
                     'implicit': True,
                     'norm': 'l1'}

    rp3b = RP3betaRecommender(urm.getCSR())
    rp3b.fit(**cf_parameters)

    if submission:
        recommended_items = rp3b.m_recommend(targetList, nRec=10)
        generate_output(targetList, recommended_items)
    else:
        cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, rp3b)
        print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}"
              .format(cumulative_precision, cumulative_recall, cumulative_MAP))

elif htype=="lfm":
    lfm = LightFMRecommender()
    lfm.fit(urm.getCSR(), epochs=100)

    if submission:
        recommended_items = lfm.m_recommend(targetList, nRec=10)
        generate_output(targetList, recommended_items)
    else:
        cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, lfm)
        print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}"
              .format(cumulative_precision, cumulative_recall, cumulative_MAP))

elif htype=="xgb":
    enable_dict = {'enableSVD': False, 'enableSLIM': False, 'enableLFM': False, 'enableP3A': False}
    xgb = XGBoostRecommender(urm, urm_t, icm, icm2, enable_dict, urm_test)
    #xgb.fit(urm, epochs=100)

    if submission:
        recommended_items = xgb.m_recommend(targetList, nRec=10)
        generate_output(targetList, recommended_items)
    else:
        cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, xgb)
        print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}"
              .format(cumulative_precision, cumulative_recall, cumulative_MAP))

elif htype=="psvd":

    psvd = PureSVDRecommender(urm.getCSR())
    psvd.fit(num_factors=225, n_iters=10)
    if submission:
        recommended_items = psvd.m_recommend(targetList, nRec=10)
        generate_output(targetList, recommended_items)
    else:
        cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, psvd)
        print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}"
              .format(cumulative_precision, cumulative_recall, cumulative_MAP))

elif htype == "bprmf":
    mf = BPRMF(num_factors=50, lrate=0.01, iters=10)
    mf.fit(urm.getCSR())
    if submission:
        recommended_items = mf.m_recommend(targetList, nRec=10)
        generate_output(targetList, recommended_items)
    else:
        cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, mf)
        print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}"
              .format(cumulative_precision, cumulative_recall, cumulative_MAP))

elif htype == "asy":
    mf = AsySVD(num_factors=50, lrate=0.01, reg=0.015, iters=10)
    mf.fit(urm.getCSR())
    if submission:
        recommended_items = mf.m_recommend(targetList, nRec=10)
        generate_output(targetList, recommended_items)
    else:
        cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, mf)
        print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}"
              .format(cumulative_precision, cumulative_recall, cumulative_MAP))

elif htype == "als":
    #225
    mf = IALS_numpy(num_factors=300,
                 reg=0.015,
                 iters=10,
                 scaling='linear',
                 alpha=40,
                 epsilon=1.0,
                 init_mean=0.0,
                 init_std=0.1)
    mf.fit(urm.getCSR())
    if submission:
        recommended_items = mf.m_recommend(targetList, nRec=10)
        generate_output(targetList, recommended_items)
    else:
        cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, mf)
        print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}"
              .format(cumulative_precision, cumulative_recall, cumulative_MAP))

elif htype == "theano":
    n_users, n_items = urm.getCSR().shape
    theano = MatrixFactorization_BPR_Theano(10, n_users, n_items)
    subset = X_train[['playlist_id', 'track_id']]
    tuples = [tuple(x) for x in subset.values]
    print(tuples[0])
    theano.train(tuples)

    test = X_test[['playlist_id', 'track_id']]
    test_tuples = [tuple(x) for x in test.values]
    theano.test(test_tuples)

    # if submission:
    #     recommended_items = theano.m_recommend(targetList, nRec=10)
    #     generate_output(targetList, recommended_items)
    # else:
    #     cumulative_precision, cumulative_recall, cumulative_MAP = evaluate_algorithm(urm_test, theano)
    #     print("Recommender, performance is: Precision = {:.4f}, Recall = {:.4f}, MAP = {:.6f}"
    #           .format(cumulative_precision, cumulative_recall, cumulative_MAP))

else:
    print("choice not supported")
