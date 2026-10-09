"""Model-aware estimates, explicitly distinct from billed tokenizer usage."""

import hashlib
import json
import math
import os
import tempfile
from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=4)
def encoding(model):
    try:
        import tiktoken
    except ImportError:
        return None, "conservative_utf8_upper_bound"
    try:
        name = tiktoken.encoding_name_for_model(model)
        method = "model_tokenizer_estimate"
    except KeyError:
        name, method = "o200k_base", "o200k_base_estimate"
    # Local context checks must never make network calls. Tokenizer assets can
    # be preinstalled; otherwise publish the conservative fallback explicitly.
    url = f"https://openaipublic.blob.core.windows.net/encodings/{name}.tiktoken"
    cache = Path(
        os.environ.get(
            "TIKTOKEN_CACHE_DIR",
            os.environ.get(
                "DATA_GYM_CACHE_DIR",
                str(Path(tempfile.gettempdir()) / "data-gym-cache"),
            ),
        )
    )
    if (
        name not in tiktoken.registry.ENCODINGS
        and not (cache / hashlib.sha1(url.encode()).hexdigest()).is_file()
    ):
        return None, "conservative_utf8_upper_bound"
    return tiktoken.get_encoding(name), method


def estimate(value, model="gpt-6-luna"):
    text = (
        value
        if isinstance(value, str)
        else json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    )
    tokenizer, method = encoding(model)
    if tokenizer is None:
        # Byte fallback is an upper bound, not an exact token count.
        count = len(text.encode())
    else:
        count = math.ceil(len(tokenizer.encode(text, disallowed_special=())) * 1.03)
    return {"estimated_tokens": count, "method": method, "model": model, "exact": False}
