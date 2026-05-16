"""
Logistic Regression baseline for PressureNet.

Linear model on the flattened (N, 45) feature matrix. Serves as the floor
baseline: if LR clears AUC > 0.63 the features carry real signal; if not,
the features or labels have a bug and no deep model will save us.
"""
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


def build_logistic_pipeline(C: float = 1.0, seed: int = 42) -> Pipeline:
    """Standardise features (LR coefficients are scale-sensitive) then fit LR."""
    return Pipeline([
        ("scaler", StandardScaler()),
        ("lr", LogisticRegression(
            C=C,
            solver="lbfgs",
            class_weight="balanced",  # handle ~58/42 imbalance
            max_iter=2000,
            random_state=seed,
        )),
    ])


def top_coefficients(pipeline: Pipeline, feature_names: list[str], k: int = 10) -> list[tuple[str, float]]:
    """Return top-k features by |coefficient| (in standardised space)."""
    coefs = pipeline.named_steps["lr"].coef_[0]
    order = np.argsort(np.abs(coefs))[::-1][:k]
    return [(feature_names[i], float(coefs[i])) for i in order]
