from pathlib import Path

from ..config import Settings
from .base import Sink
from .local import LocalSink
from .s3 import S3Sink

SINKS = ("local", "s3")


def get_sink(name: str, cfg: Settings) -> Sink:
    if name == "local":
        return LocalSink(Path(cfg.output_path), cfg.output_format)
    if name == "s3":
        return S3Sink(cfg.s3_bucket, cfg.s3_prefix, cfg.aws_region)
    raise ValueError(f"unknown sink: {name}. Available: {', '.join(SINKS)}")


__all__ = ["SINKS", "LocalSink", "S3Sink", "Sink", "get_sink"]
