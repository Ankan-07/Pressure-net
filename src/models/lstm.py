"""
Bidirectional LSTM baseline for PressureNet.

Architecture follows the spec in CLAUDE.md:
    Linear(15, 32) -> bi-LSTM(hidden=64, 1 layer) -> Dropout(0.2) -> Linear(128, 1)

The model emits a single logit per event (no sigmoid — use BCEWithLogitsLoss).

Padding handling
----------------
Our temporal window is built with padding at the START of the sequence:
    if num_real == 3: [t-2, t-1, t=0]   ← no padding
    if num_real == 2: [PAD, t-1, t=0]
    if num_real == 1: [PAD, PAD, t=0]
PyTorch's `pack_padded_sequence` requires padding at the END. We reverse the
input along the time axis so real timesteps come first, then pack:
    num_real=3 -> [t=0, t-1, t-2]
    num_real=2 -> [t=0, t-1, PAD]
    num_real=1 -> [t=0, PAD, PAD]
The bi-LSTM still sees every real timestep; only the conventional ordering
flips. h_n (final hidden state per direction) is unaffected by padded steps
because packing skips them entirely.
"""
import torch
import torch.nn as nn
from torch.nn.utils.rnn import pack_padded_sequence


class PressureLSTM(nn.Module):
    def __init__(self, n_features: int = 15, proj_dim: int = 32,
                 hidden_size: int = 64, dropout: float = 0.2):
        super().__init__()
        self.input_proj = nn.Linear(n_features, proj_dim)
        self.lstm = nn.LSTM(
            input_size=proj_dim,
            hidden_size=hidden_size,
            num_layers=1,
            batch_first=True,
            bidirectional=True,
            dropout=0.0,
        )
        self.head = nn.Sequential(
            nn.Dropout(dropout),
            nn.Linear(hidden_size * 2, 1),   # bidirectional => hidden * 2
        )

    def forward(self, x: torch.Tensor, num_real: torch.Tensor) -> torch.Tensor:
        """
        x:        (batch, T=3, F=15) — standardised features, padded at start
        num_real: (batch,) long      — number of real timesteps per item (1..3)
        returns:  (batch,) float     — raw logits
        """
        # Reverse along time so real timesteps come FIRST (pack expects this)
        x_rev = torch.flip(x, dims=[1])              # (batch, T, F)
        h = self.input_proj(x_rev)                   # (batch, T, proj_dim)

        # Pack: enforce_sorted=False lets us pass unsorted lengths.
        # Lengths must be on CPU per the torch API.
        packed = pack_padded_sequence(
            h, lengths=num_real.cpu(), batch_first=True, enforce_sorted=False
        )
        _, (h_n, _) = self.lstm(packed)              # h_n: (2, batch, hidden)

        # Concat both directions' final hidden states
        fwd = h_n[0]                                  # (batch, hidden)
        bwd = h_n[1]                                  # (batch, hidden)
        pooled = torch.cat([fwd, bwd], dim=-1)        # (batch, 2*hidden)

        return self.head(pooled).squeeze(-1)          # (batch,)
