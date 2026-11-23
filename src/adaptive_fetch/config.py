from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="AF_",
        env_file=".env",
        extra="ignore",
    )

    input_path: Path = Path("input.csv")
    output_path: Path = Path("output")
    base_url: str = "http://localhost:8080"
    record_path: str = "/records/{id}"

    batch_size: int = 0
    min_batch_size: int = 8
    max_batch_size: int = 512
    max_concurrent: int = 8

    memory_low_pct: float = 60.0
    memory_high_pct: float = 80.0
    memory_critical_pct: float = 90.0
    growth_patience: int = 3

    disk_warn_mb: float = 500.0
    disk_halt_mb: float = 100.0

    latency_fast_ms: float = 200.0
    latency_slow_ms: float = 800.0
    probe_url: str = ""

    timeout_seconds: float = 30.0
    retries: int = 2

    checkpoint_dir: Path = Path(".checkpoints")
    checkpoint_every: int = 25
    resume: bool = True

    sink: str = "local"
    output_format: str = "json"
    s3_bucket: str = ""
    s3_prefix: str = "adaptive-fetch"
    aws_region: str = "us-east-1"

    log_level: str = "INFO"


settings = Settings()
