"""Load and expose the NSCLC entity vocabulary from entities.yaml.

Public API
----------
load_entities()        -> full parsed dict
get_condition_aliases() -> list[str]
get_mesh_codes()       -> list[str]
get_modalities()       -> dict[str, dict]
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

_YAML_PATH = Path(__file__).with_name("entities.yaml")

_cache: dict[str, Any] | None = None


def load_entities() -> dict[str, Any]:
    """Return the full entities dict, cached after first load."""
    global _cache  # noqa: PLW0603
    if _cache is None:
        with open(_YAML_PATH, "r") as fh:
            _cache = yaml.safe_load(fh)
    return _cache


def get_mesh_codes() -> list[str]:
    """Return MeSH descriptor codes for NSCLC conditions."""
    return list(load_entities()["conditions"]["mesh_codes"])


def get_condition_aliases() -> list[str]:
    """Return case-insensitive alias patterns for NSCLC conditions."""
    return list(load_entities()["conditions"]["aliases"])


def get_modalities() -> dict[str, dict[str, Any]]:
    """Return the modalities mapping (name -> {atc_prefixes, drug_aliases})."""
    return dict(load_entities()["modalities"])
