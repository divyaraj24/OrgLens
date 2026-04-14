from __future__ import annotations

from dataclasses import dataclass, field
import os

from orglens.config import load_config


@dataclass
class ObservabilityLlmSettings:
    enabled: bool = False
    provider: str = "openai_compatible"
    api_url: str = "https://api.openai.com/v1/chat/completions"
    api_key: str | None = None
    model: str = "gpt-4o-mini"
    temperature: float = 0.2
    max_tokens: int = 700
    timeout_seconds: float = 20.0


@dataclass
class ObservabilitySettings:
    pg_dsn: str = "postgresql://orglens:password@localhost:5432/orglens"
    host: str = "0.0.0.0"
    port: int = 8010
    llm: ObservabilityLlmSettings = field(default_factory=ObservabilityLlmSettings)


def load_observability_settings(config_path: str | None = None) -> ObservabilitySettings:
    cfg = load_config(config_path)
    l2 = cfg.get("layer2", {})
    pg_cfg = l2.get("postgres", {})

    observability = cfg.get("layer2_observability", {})
    api_cfg = observability.get("api", {})
    llm_cfg = observability.get("llm", {})

    return ObservabilitySettings(
        pg_dsn=pg_cfg.get("dsn", "postgresql://orglens:password@localhost:5432/orglens"),
        host=api_cfg.get("host", "0.0.0.0"),
        port=int(api_cfg.get("port", 8010)),
        llm=ObservabilityLlmSettings(
            enabled=str(
                os.environ.get("ORGLENS_LLM_ENABLED", llm_cfg.get("enabled", "false"))
            ).lower()
            in {"1", "true", "yes", "on"},
            provider=os.environ.get(
                "ORGLENS_LLM_PROVIDER", llm_cfg.get("provider", "openai_compatible")
            ),
            api_url=os.environ.get(
                "ORGLENS_LLM_API_URL",
                llm_cfg.get("api_url", "https://api.openai.com/v1/chat/completions"),
            ),
            api_key=os.environ.get("ORGLENS_LLM_API_KEY", llm_cfg.get("api_key")),
            model=os.environ.get("ORGLENS_LLM_MODEL", llm_cfg.get("model", "gpt-4o-mini")),
            temperature=float(
                os.environ.get("ORGLENS_LLM_TEMPERATURE", llm_cfg.get("temperature", 0.2))
            ),
            max_tokens=int(
                os.environ.get("ORGLENS_LLM_MAX_TOKENS", llm_cfg.get("max_tokens", 700))
            ),
            timeout_seconds=float(
                os.environ.get("ORGLENS_LLM_TIMEOUT_SECONDS", llm_cfg.get("timeout_seconds", 20.0))
            ),
        ),
    )


# Backward-compatible alias for pre-simplification imports.
load_layer5_settings = load_observability_settings
