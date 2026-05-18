from __future__ import annotations

from contextlib import contextmanager
from typing import List

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset

from src.models.transformer import PressureTransformer


@contextmanager
def capture_cls_attention(model: PressureTransformer, layer_idx: int = 0):
    captured: List[torch.Tensor] = []
    layer = model.encoder.layers[layer_idx]
    original_forward = layer.forward
    original_enable_nested = model.encoder.enable_nested_tensor

    def slow_forward(src, src_mask=None, src_key_padding_mask=None, is_causal=False):
                                                                         
                                                                      
        x = src
        attn_out, weights = layer.self_attn(
            x, x, x,
            attn_mask=src_mask,
            key_padding_mask=src_key_padding_mask,
            need_weights=True,
            average_attn_weights=False,
            is_causal=is_causal,
        )
        captured.append(weights.detach().cpu())
        x = layer.norm1(x + layer.dropout1(attn_out))
        x = layer.norm2(x + layer._ff_block(x))
        return x

    layer.forward = slow_forward
    model.encoder.enable_nested_tensor = False
    try:
        yield captured
    finally:
        layer.forward = original_forward
        model.encoder.enable_nested_tensor = original_enable_nested


@torch.no_grad()
def extract_attention_heatmaps(model: PressureTransformer,
                               dataset: Dataset,
                               batch_size: int = 256) -> np.ndarray:
    device = next(model.parameters()).device
    model.eval()
    loader = DataLoader(dataset, batch_size=batch_size, shuffle=False)

    heatmaps: List[np.ndarray] = []
    with capture_cls_attention(model, layer_idx=0) as captured:
        for batch in loader:
            x = batch["x"].to(device)
            pad_mask = batch["pad_mask"].to(device)
            _ = model(x, pad_mask)

                                                             
            w = captured.pop()                                      
            cls_row = w[:, :, 0, 1:]                                                      
            cls_attn = cls_row.mean(dim=1)                                 

                                                                   
            cls_attn = cls_attn.masked_fill(pad_mask.cpu(), 0.0)

                                                                             
            x_abs = x.detach().cpu().abs()                             
            heat = cls_attn.unsqueeze(-1) * x_abs                      
            heatmaps.append(heat.numpy())

    return np.concatenate(heatmaps, axis=0)


def aggregate_by_player(heatmaps: np.ndarray, player_ids: np.ndarray
                        ) -> tuple[np.ndarray, np.ndarray]:
    unique_ids, inverse = np.unique(player_ids, return_inverse=True)
    P, _, T, F = len(unique_ids), 0, heatmaps.shape[1], heatmaps.shape[2]
    sums = np.zeros((P, T, F), dtype=np.float64)
    counts = np.zeros((P,), dtype=np.int64)
    for i, idx in enumerate(inverse):
        sums[idx] += heatmaps[i]
        counts[idx] += 1
    means = (sums / counts[:, None, None]).astype(np.float32)
    return unique_ids.astype(np.int64), means
