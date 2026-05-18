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
            nn.Linear(hidden_size * 2, 1),                                
        )

    def forward(self, x: torch.Tensor, num_real: torch.Tensor) -> torch.Tensor:
                                                                             
        x_rev = torch.flip(x, dims=[1])                             
        h = self.input_proj(x_rev)                                         

                                                                   
        packed = pack_padded_sequence(
            h, lengths=num_real.cpu(), batch_first=True, enforce_sorted=False
        )
        _, (h_n, _) = self.lstm(packed)                                       

                                                     
        fwd = h_n[0]                                                   
        bwd = h_n[1]                                                   
        pooled = torch.cat([fwd, bwd], dim=-1)                           

        return self.head(pooled).squeeze(-1)                    
