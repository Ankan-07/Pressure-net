from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from sklearn.linear_model import LogisticRegression


_EPS = 1e-6


def probs_to_logits(p: np.ndarray) -> np.ndarray:
    p = np.clip(np.asarray(p, dtype=np.float64), _EPS, 1.0 - _EPS)
    return np.log(p / (1.0 - p))


def sigmoid(z: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-z))


@dataclass
class PlattCalibrator:
    a: float = 1.0
    b: float = 0.0
    n_fit: int = 0

    def fit(self, scores: np.ndarray, labels: np.ndarray,
            score_type: str = "logit") -> "PlattCalibrator":
        if score_type == "prob":
            z = probs_to_logits(scores)
        elif score_type == "logit":
            z = np.asarray(scores, dtype=np.float64)
        else:
            raise ValueError(f"score_type must be 'logit' or 'prob', got {score_type!r}")

                                                                                
        lr = LogisticRegression(C=1e6, solver="lbfgs", max_iter=1000)
        lr.fit(z.reshape(-1, 1), np.asarray(labels, dtype=np.int64))

        self.a = float(lr.coef_.ravel()[0])
        self.b = float(lr.intercept_.ravel()[0])
        self.n_fit = int(len(labels))
        return self

    def predict_proba(self, scores: np.ndarray,
                      score_type: str = "logit") -> np.ndarray:
        if score_type == "prob":
            z = probs_to_logits(scores)
        elif score_type == "logit":
            z = np.asarray(scores, dtype=np.float64)
        else:
            raise ValueError(f"score_type must be 'logit' or 'prob', got {score_type!r}")
        return sigmoid(self.a * z + self.b)

    def to_dict(self) -> dict:
        return {"a": self.a, "b": self.b, "n_fit": self.n_fit}

    @classmethod
    def from_dict(cls, d: dict) -> "PlattCalibrator":
        return cls(a=float(d["a"]), b=float(d["b"]), n_fit=int(d.get("n_fit", 0)))
