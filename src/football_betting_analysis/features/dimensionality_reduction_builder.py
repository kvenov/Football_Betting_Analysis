from sklearn.decomposition import (
    PCA,
    KernelPCA,
    TruncatedSVD,
    FastICA,
)

class DimensionalityReductionBuilder:
    """
        Builds sklearn dimensionality reduction transformers.

        Supported methods:
            - PCA
            - KernelPCA
            - TruncatedSVD
            - FastICA
    """

    def __init__(self, config: dict):
        self.config = config

    def build(self):

        method = self.config["method"]

        if method == "PCA":
            return self._build_pca()

        elif method == "KernelPCA":
            return self._build_kernel_pca()

        elif method == "TruncatedSVD":
            return self._build_truncated_svd()

        elif method == "FastICA":
            return self._build_fast_ica()

        else:
            raise ValueError(
                f"Unknown dimensionality reduction method: {method}"
            )

    
    # Functions for building the algorithms:
    def _build_pca(self):

        return PCA(
            n_components=self.config.get(
                "n_components",
                0.95,
            ),
            whiten=self.config.get(
                "whiten",
                False,
            ),
            svd_solver=self.config.get(
                "svd_solver",
                "auto",
            ),
            random_state=self.config.get(
                "random_state",
                42,
            )
        )

    def _build_kernel_pca(self):

        return KernelPCA(
            n_components=self.config.get(
                "n_components",
                None,
            ),
            kernel=self.config.get(
                "kernel",
                "rbf",
            ),
            gamma=self.config.get(
                "gamma",
                None,
            ),
            degree=self.config.get(
                "degree",
                3,
            ),
            coef0=self.config.get(
                "coef0",
                1,
            ),
            fit_inverse_transform=self.config.get(
                "fit_inverse_transform",
                False,
            ),
            eigen_solver=self.config.get(
                "eigen_solver",
                "auto",
            ),
            n_jobs=self.config.get(
                "n_jobs",
                -1,
            )
        )

    def _build_truncated_svd(self):

        return TruncatedSVD(
            n_components=self.config.get(
                "n_components",
                100,
            ),
            algorithm=self.config.get(
                "algorithm",
                "randomized",
            ),
            n_iter=self.config.get(
                "n_iter",
                7,
            ),
            random_state=self.config.get(
                "random_state",
                42,
            )
        )

    def _build_fast_ica(self):

        return FastICA(
            n_components=self.config.get(
                "n_components",
                None,
            ),
            algorithm=self.config.get(
                "algorithm",
                "parallel",
            ),
            whiten=self.config.get(
                "whiten",
                "unit-variance",
            ),
            fun=self.config.get(
                "fun",
                "logcosh",
            ),
            max_iter=self.config.get(
                "max_iter",
                500,
            ),
            tol=self.config.get(
                "tol",
                1e-4,
            ),
            random_state=self.config.get(
                "random_state",
                42,
            )
        )