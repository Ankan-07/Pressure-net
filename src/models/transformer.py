"""
Transformer encoder for PressureNet (BERT-style, encoder-only).

Architecture follows the spec in CLAUDE.md:
    Linear(15, 64) -> prepend learnable [CLS] -> add learned positional embed (len 4)
    -> 2 x TransformerEncoderLayer(d_model=64, nhead=4, ff=128, dropout=0.1, GELU)
    -> extract [CLS] hidden -> Dropout(0.2) -> Linear(64, 1)

The [CLS] hidden state at the top of the encoder is the pooled sequence
representation that the classification head consumes. Position 0 is reserved
for [CLS]; positions 1..3 carry the (T=3) timesteps in chronological order
[t-2, t-1, t=0].

Padding handling
----------------
Self-attention is order-agnostic, so we keep the natural chronological order
and use `src_key_padding_mask` to tell every attention layer to ignore padded
positions. Mask shape is (batch, 1 + T) — False for [CLS] (always real) plus
the per-event `pad_mask`. The learned positional embedding can still be
applied at padded positions; the mask prevents any attention weight from
touching them.

For Week 6 explainability we register a forward hook on `encoder.layers[0]
.self_attn` (need_weights=True) to extract the [CLS] token's attention row.
That row tells us which (timestep, feature) pairs the model leaned on for
each prediction — the foundation of the per-player attention heatmap.
"""
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
        self.pos_embed = nn.Parameter(torch.zeros(1, seq_len + 1, d_model))   # +1 for CLS

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

        # Truncated-normal init — standard for learned PE / CLS tokens
        nn.init.trunc_normal_(self.cls_token, std=0.02)
        nn.init.trunc_normal_(self.pos_embed, std=0.02)

    def forward(self, x: torch.Tensor, pad_mask: torch.Tensor) -> torch.Tensor:
        """
        x:        (batch, T=3, F=15)  — standardised features in chronological order
        pad_mask: (batch, T=3) bool   — True where padded (matches PressureDataset)
        returns:  (batch,) float      — raw logits
        """
        b = x.size(0)
        h = self.input_proj(x)                                       # (b, T, d)

        # Prepend [CLS] token, then add positional embedding to all 4 positions
        cls = self.cls_token.expand(b, -1, -1)                       # (b, 1, d)
        h = torch.cat([cls, h], dim=1) + self.pos_embed              # (b, T+1, d)

        # Build key-padding mask: False for CLS (never padded) + per-event pad_mask
        # nn.Transformer convention: True = "ignore this key"
        cls_mask = torch.zeros(b, 1, dtype=torch.bool, device=x.device)
        key_padding_mask = torch.cat([cls_mask, pad_mask], dim=1)    # (b, T+1)

        h = self.encoder(h, src_key_padding_mask=key_padding_mask)   # (b, T+1, d)

        cls_out = h[:, 0]                                            # (b, d) — pooled
        return self.head(cls_out).squeeze(-1)                        # (b,)
