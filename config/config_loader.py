"""
config/config_loader.py
=======================
Single source of truth for app_config.json.
Import `get_config()` anywhere to read the current configuration.
Import `save_config()` to persist changes (used by the webapp API).
"""
from __future__ import annotations

import json
import pathlib
from typing import Any

_CONFIG_PATH = pathlib.Path(__file__).with_name("app_config.json")


def get_config() -> dict[str, Any]:
    """Return the current configuration as a plain dict (always fresh from disk)."""
    return json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))


def save_config(cfg: dict[str, Any]) -> None:
    """Persist a (possibly modified) config dict back to disk."""
    _CONFIG_PATH.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")
