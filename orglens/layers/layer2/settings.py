from __future__ import annotations

from dataclasses import dataclass

from orglens.config import load_config


@dataclass
class Layer2Settings:
    mode: str = "api"
    api_host: str = "0.0.0.0"
    api_port: int = 8001
    api_key: str = ""
    api_signing_secret: str = ""
    api_signature_tolerance_seconds: int = 300
    api_queue_max_retries: int = 3
    api_queue_retry_base_seconds: int = 5
    api_queue_poll_interval_seconds: float = 0.5

    redis_url: str = "redis://localhost:6379/0"
    redis_stream: str = "orglens:ingest"
    redis_group: str = "orglens:workers"
    redis_consumer: str = "worker-1"

    pg_dsn: str = "postgresql://orglens:password@localhost:5432/orglens"

    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "raw-events"
    minio_secure: bool = False

    input_file: str | None = None
    replay_repo: str | None = None
    from_date: str | None = None


def load_layer2_settings(config_path: str | None = None) -> Layer2Settings:
    cfg = load_config(config_path)
    l2 = cfg.get("layer2", {})

    redis_cfg = l2.get("redis", {})
    pg_cfg = l2.get("postgres", {})
    api_cfg = l2.get("api", {})
    minio_cfg = l2.get("minio", {})

    return Layer2Settings(
        api_host=api_cfg.get("host", "0.0.0.0"),
        api_port=int(api_cfg.get("port", 8001)),
        api_key=api_cfg.get("api_key", ""),
        api_signing_secret=api_cfg.get("signing_secret", ""),
        api_signature_tolerance_seconds=int(api_cfg.get("signature_tolerance_seconds", 300)),
        api_queue_max_retries=int(api_cfg.get("queue_max_retries", 3)),
        api_queue_retry_base_seconds=int(api_cfg.get("queue_retry_base_seconds", 5)),
        api_queue_poll_interval_seconds=float(api_cfg.get("queue_poll_interval_seconds", 0.5)),
        redis_url=redis_cfg.get("url", "redis://localhost:6379/0"),
        redis_stream=redis_cfg.get("stream", "orglens:ingest"),
        redis_group=redis_cfg.get("group", "orglens:workers"),
        redis_consumer=redis_cfg.get("consumer", "worker-1"),
        pg_dsn=pg_cfg.get("dsn", "postgresql://orglens:password@localhost:5432/orglens"),
        minio_endpoint=minio_cfg.get("endpoint", "localhost:9000"),
        minio_access_key=minio_cfg.get("access_key", "minioadmin"),
        minio_secret_key=minio_cfg.get("secret_key", "minioadmin"),
        minio_bucket=minio_cfg.get("bucket", "raw-events"),
        minio_secure=bool(minio_cfg.get("secure", False)),
    )
