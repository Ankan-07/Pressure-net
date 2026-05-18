import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


def build_logistic_pipeline(C: float = 1.0, seed: int = 42) -> Pipeline:
    return Pipeline([
        ("scaler", StandardScaler()),
        ("lr", LogisticRegression(
            C=C,
            solver="lbfgs",
            class_weight="balanced",                           
            max_iter=2000,
            random_state=seed,
        )),
    ])


def top_coefficients(pipeline: Pipeline, feature_names: list[str], k: int = 10) -> list[tuple[str, float]]:
    coefs = pipeline.named_steps["lr"].coef_[0]
    order = np.argsort(np.abs(coefs))[::-1][:k]
    return [(feature_names[i], float(coefs[i])) for i in order]
