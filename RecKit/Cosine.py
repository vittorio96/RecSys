import scipy
import numpy as np
import scipy.sparse as sps
from RecKit.check_matrix import check_matrix
class ISimilarity(object):
    """Abstract interface for the similarity metrics"""

    def __init__(self, shrinkage=10):
        self.shrinkage = shrinkage

    def compute(self, icm):
        pass


class Cosine(ISimilarity):

    def compute(self, icm):
        """
                USED FOR CONTENT BASED FILTERING
        """
        # convert to csc matrix for faster column-wise operations
        X = check_matrix(icm, 'csc', dtype=np.float32)

        # 1) normalize the columns in icm
        # compute the column-wise norm
        # NOTE: this is slightly inefficient. We must copy icm to compute the column norms.
        # A faster solution is to  normalize the matrix inplace with a Cython function.
        Xsq = X.copy()
        Xsq.data = Xsq.data ** 2
        norm = np.sqrt(Xsq.sum(axis=0))
        norm = np.asarray(norm).ravel()
        norm += 1e-6
        # compute the number of non-zeros in each column
        # NOTE: this works only if X is instance of sparse.csc_matrix
        col_nnz = np.diff(X.indptr)
        # then normalize the values in each column
        X.data = X.data / np.repeat(norm, col_nnz)
        print("Normalized")

        # 2) compute the cosine similarity using the dot-product
        dist = X * X.T
        print("Computed")

        # zero out diagonal values
        dist = dist - sps.dia_matrix((dist.diagonal()[scipy.newaxis, :], [0]), shape=dist.shape)
        print("Removed diagonal")

        # and apply the shrinkage
        if self.shrinkage > 0:
            dist = self.apply_shrinkage(icm, dist)
            print("Applied shrinkage")

        return dist

    def apply_shrinkage(self, X, dist):
        # create an "indicator" version of X (i.e. replace values in X with ones)
        icm_ind = X.getCSC().copy()
        icm_ind.data = np.ones_like(icm_ind.data)
        # compute the co-rated counts
        co_counts = icm_ind * icm_ind.T
        # remove the diagonal
        co_counts = co_counts - sps.dia_matrix((co_counts.diagonal()[scipy.newaxis, :], [0]), shape=co_counts.shape)
        # compute the shrinkage factor as co_counts_ij / (co_counts_ij + shrinkage)
        # then multiply dist with it
        co_counts_shrink = co_counts.copy()
        co_counts_shrink.data = co_counts_shrink.data + self.shrinkage
        co_counts.data = co_counts.data / co_counts_shrink.data
        dist.data = dist.data * co_counts.data
        return dist