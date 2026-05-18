import torch
import torch.nn as nn


class PressureTransformer(nn.Module):
    def __init__(self, n_features: int = 15, seq_len: int = 3,
                 d_model: int = 64, nhead: int = 4, dim_ff: int = 128,
                 num_layers: int = 2, attn_dropout: float = 0.1,
                 head_dropout: float = 0.2):
        super().__init__()
        self.seq_len = seq_len

        self.input_proj = nn.Linear(n_features, d_model)
        self.cls_token = nn.Parameter(torch.zeros(1, 1, d_model))
        self.pos_embed = nn.Parameter(torch.zeros(1, seq_len + 1, d_model))               

        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model,
            nhead=nhead,
            dim_feedforward=dim_ff,
            dropout=attn_dropout,
            activation="gelu",
            batch_first=True,
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)

        self.head = nn.Sequential(
            nn.Dropout(head_dropout),
            nn.Linear(d_model, 1),
        )

                                                                      
        nn.init.trunc_normal_(self.cls_token, std=0.02)
        nn.init.trunc_normal_(self.pos_embed, std=0.02)

    def forward(self, x: torch.Tensor, pad_mask: torch.Tensor) -> torch.Tensor:
        b = x.size(0)
        h = self.input_proj(x)                                                  

                                                                               
        cls = self.cls_token.expand(b, -1, -1)                                  
        h = torch.cat([cls, h], dim=1) + self.pos_embed                           

                                                                                   
        cls_mask = torch.zeros(b, 1, dtype=torch.bool, device=x.device)
        key_padding_mask = torch.cat([cls_mask, pad_mask], dim=1)              

        h = self.encoder(h, src_key_padding_mask=key_padding_mask)                

        cls_out = h[:, 0]                                                             
        return self.head(cls_out).squeeze(-1)                              
