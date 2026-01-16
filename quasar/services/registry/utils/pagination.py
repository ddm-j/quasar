"""Pagination utilities for Registry API endpoints."""

import base64
import json


def encode_cursor(score: float, src_sym: str, tgt_sym: str) -> str:
    """Encode pagination cursor as base64 JSON.

    Args:
        score: The score value of the last item.
        src_sym: Source symbol of the last item.
        tgt_sym: Target symbol of the last item.

    Returns:
        Base64-encoded cursor string.
    """
    return base64.urlsafe_b64encode(
        json.dumps([score, src_sym, tgt_sym]).encode()
    ).decode()


def decode_cursor(cursor: str) -> tuple[float, str, str]:
    """Decode pagination cursor from base64 JSON.

    Args:
        cursor: Base64-encoded cursor string.

    Returns:
        Tuple of (score, source_symbol, target_symbol).

    Raises:
        ValueError: If cursor is malformed.
    """
    try:
        data = json.loads(base64.urlsafe_b64decode(cursor))
        return (float(data[0]), str(data[1]), str(data[2]))
    except Exception as e:
        raise ValueError(f"Invalid cursor format: {e}")
