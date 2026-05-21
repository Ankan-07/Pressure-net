# Base: Debian 12 + Python 3.11, "slim" = no docs/build extras.
FROM python:3.11-slim-bookworm

WORKDIR /app

# uv is the project's package manager. Pinned for reproducible builds.
RUN pip install --no-cache-dir uv==0.5.29

# Tell uv to fetch the CPU-only torch wheels instead of the default
# CUDA build — saves ~1.5 GB in the final image.
ENV UV_TORCH_BACKEND=cpu

# === Dependency layer (changes rarely — cached across rebuilds) ===
# Copy only the manifests first so Docker can cache the install step.
# If you only change app code, this layer is reused on rebuild.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev

# === App layer (changes often) ===
# Copy code and the runtime artefacts the API loads at startup.
# Listed explicitly so we don't accidentally ship raw data or notebooks.
COPY api/ ./api/
COPY src/ ./src/
COPY data/checkpoints/transformer.pt ./data/checkpoints/
COPY data/features/features.parquet ./data/features/
COPY results/calibration/transformer.json ./results/calibration/
COPY outputs/ ./outputs/

# HF Spaces requires apps to listen on 7860.
EXPOSE 7860

CMD ["uv", "run", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "7860"]
