"""
Config loader — reads config.yaml, interpolates ${ENV_VAR} references,
and returns a typed dict ready for use by all subsystems.
"""
from __future__ import annotations

import os
import re
import logging
from pathlib import Path
import urllib.parse
from typing import Any, Dict

import yaml

log = logging.getLogger(__name__)

_ENV_VAR_RE = re.compile(r"\$\{([^}]+)\}")


def _interpolate(value: Any) -> Any:
    """Recursively substitute ${VAR} with os.environ values."""
    if isinstance(value, str):
        def _replace(m: re.Match) -> str:
            var = m.group(1)
            return os.environ.get(var, m.group(0))  # leave as-is if not found
        return _ENV_VAR_RE.sub(_replace, value)
    if isinstance(value, dict):
        return {k: _interpolate(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate(item) for item in value]
    return value


def _normalize_repos(repos: list) -> list:
    """If a repo only has a 'url' (or is a string), infer 'owner', 'repo', and 'git_url'."""
    normalized = []
    for r in repos:
        if isinstance(r, str):
            r = {"url": r}
        elif not isinstance(r, dict):
            continue
            
        if "url" in r and not ("owner" in r and "repo" in r):
            url = r["url"]
            cleaned_url = url[:-4] if url.endswith(".git") else url
            parts = urllib.parse.urlparse(cleaned_url).path.strip("/").split("/")
            if len(parts) >= 2:
                r["owner"] = parts[-2]
                r["repo"] = parts[-1]
            if "git_url" not in r:
                r["git_url"] = url if url.endswith(".git") else f"{url}.git"
                
        normalized.append(r)
    return normalized


def load_config(path: str | None = None) -> Dict[str, Any]:
    """
    Load and return the agent config.

    Resolution order:
      1. `path` argument
      2. ORGLENS_CONFIG environment variable
      3. ./config.local.yaml
      4. ./config.yaml
    """
    candidates = []
    if path:
        candidates.append(path)
    env_path = os.environ.get("ORGLENS_CONFIG")
    if env_path:
        candidates.append(env_path)
    candidates += ["config.local.yaml", "config.yaml"]

    for candidate in candidates:
        p = Path(candidate)
        if p.exists():
            log.info("Loading config from %s", p)
            with p.open() as f:
                raw = yaml.safe_load(f)
            config_dict = _interpolate(raw or {})
            if "repos" in config_dict and isinstance(config_dict["repos"], list):
                config_dict["repos"] = _normalize_repos(config_dict["repos"])
            return config_dict

    raise FileNotFoundError(
        "No config file found. Tried: " + ", ".join(candidates)
    )
