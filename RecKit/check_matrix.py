import numpy as np
import scipy.sparse as sps
def check_matrix(X, format='csc', dtype=np.float32):
    if format == 'csc' and not isinstance(X, sps.csc_matrix):
        return X.getCSC()
    if format == 'csr' and not isinstance(X, sps.csr_matrix):
        return X.getCSR()
    if format == 'coo' and not isinstance(X, sps.coo_matrix):
        return X.getCoord()
    else : return X

def check_matrix_base(X, format='csc', dtype=np.float32):
    if format == 'csc' and not isinstance(X, sps.csc_matrix):
        return X.tocsc()
    if format == 'csr' and not isinstance(X, sps.csr_matrix):
        return X.tocsr()
    if format == 'coo' and not isinstance(X, sps.coo_matrix):
        return X.tocoo()
    else : return X