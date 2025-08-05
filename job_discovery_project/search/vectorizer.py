from typing import List

from sentence_transformers import SentenceTransformer

_model: SentenceTransformer | None = None


def get_model() -> SentenceTransformer:
    global _model
    if _model is None:
        _model = SentenceTransformer("all-MiniLM-L6-v2")
    return _model


def embed(text: str) -> List[float]:
    """Return an embedding vector for the provided text."""
    model = get_model()
    return model.encode([text])[0].tolist()
