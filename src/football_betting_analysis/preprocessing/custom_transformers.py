import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin


class Log1pTransformer(BaseEstimator, TransformerMixin):
    """
        Applies numpy.log1p(x) element-wise.
        Useful for positively skewed numerical features
    """

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X = np.asarray(X, dtype=np.float64)
        return np.log1p(X)

    def get_feature_names_out(self, input_features=None):
        return input_features


class SqrtTransformer(BaseEstimator, TransformerMixin):
    """
    Applies square root transformation element-wise.

    Negative values are clipped to zero before
    applying sqrt to avoid invalid values.
    """

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X = np.asarray(X, dtype=np.float64)
        X = np.clip(X, a_min=0, a_max=None)
        return np.sqrt(X)

    def get_feature_names_out(self, input_features=None):
        return input_features