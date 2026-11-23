import asyncio
import logging
from pathlib import Path

import click

from . import __version__
from .config import Settings
from .core.fetcher import Fetcher, limits_from_settings, load_ids
from .core.governor import ResourceGovernor
from .core.resources import initial_batch_size, read_memory, snapshot
from .sinks import SINKS, get_sink


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    for noisy in ("httpx", "httpcore", "botocore", "boto3", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


@click.group()
@click.version_option(__version__)
def main() -> None:
    """Fetch records in batches that re-tune themselves under resource pressure."""


@main.command()
@click.argument("input_path", type=click.Path(path_type=Path))
@click.option("--output", type=click.Path(path_type=Path), default=Path("output"))
@click.option("--base-url", default=None, help="Service base URL.")
@click.option("--sink", type=click.Choice(SINKS), default=None)
@click.option("--format", "fmt", type=click.Choice(["json", "csv", "excel"]), default=None)
@click.option("--batch-size", type=int, default=None, help="0 or omit to auto-size from memory.")
@click.option("--concurrent", type=int, default=None)
@click.option("--s3-bucket", default=None)
@click.option("--s3-prefix", default=None)
@click.option("--resume/--no-resume", default=None)
@click.option("--log-level", default=None)
def run(
    input_path: Path,
    output: Path,
    base_url: str | None,
    sink: str | None,
    fmt: str | None,
    batch_size: int | None,
    concurrent: int | None,
    s3_bucket: str | None,
    s3_prefix: str | None,
    resume: bool | None,
    log_level: str | None,
) -> None:
    """Fetch every record listed in INPUT_PATH."""
    overrides = {
        "input_path": input_path,
        "output_path": output,
        "base_url": base_url,
        "sink": sink,
        "output_format": fmt,
        "batch_size": batch_size,
        "max_concurrent": concurrent,
        "s3_bucket": s3_bucket,
        "s3_prefix": s3_prefix,
        "resume": resume,
        "log_level": log_level,
    }
    cfg = Settings(**{k: v for k, v in overrides.items() if v is not None})
    _configure_logging(cfg.log_level)

    destination = get_sink(cfg.sink, cfg)
    if not destination.is_available():
        raise click.ClickException(
            f"sink '{cfg.sink}' is not usable. For s3, check the bucket name and credentials."
        )

    report = asyncio.run(Fetcher(cfg).run())
    written = destination.write(
        [r for r in _records(cfg) if r], report.as_summary()
    )

    click.echo("")
    click.echo(f"total     {report.total}")
    click.echo(f"fetched   {report.fetched}")
    click.echo(f"errors    {report.errors}")
    click.echo(f"skipped   {report.skipped}  (already in checkpoint)")
    click.echo(f"batches   {report.batches}  sizes={report.batch_sizes}")
    click.echo(f"recycles  {report.recycles}")
    click.echo(f"duration  {report.duration_seconds:.1f}s")
    if report.halted_reason:
        click.echo(f"HALTED    {report.halted_reason}")
    for path in written:
        click.echo(f"wrote     {path}")


def _records(cfg: Settings) -> list[dict]:
    from .core.checkpoint import Checkpoint

    return Checkpoint(cfg.checkpoint_dir, cfg.checkpoint_every).records


@main.command()
@click.option("--probe-url", default=None, help="Host to time a TCP connect against.")
@click.option("--path", type=click.Path(path_type=Path), default=Path("."))
def probe(probe_url: str | None, path: Path) -> None:
    """Show the current resource snapshot and how the governor reads it."""
    cfg = Settings(probe_url=probe_url) if probe_url else Settings()
    snap = snapshot(path, cfg.probe_url)

    click.echo(f"memory used       {snap.memory_percent:.1f}%")
    click.echo(f"memory available  {snap.memory_available_mb:.0f}MB")
    click.echo(f"process rss       {snap.process_rss_mb:.0f}MB")
    click.echo(f"disk free         {snap.disk_free_mb:.0f}MB")
    click.echo(
        f"latency           {f'{snap.latency_ms:.0f}ms' if snap.latency_known else 'not probed'}"
    )

    governor = ResourceGovernor(limits_from_settings(cfg), starting_batch_size=64, total_items=100)
    decision = governor.evaluate(snap)
    click.echo("")
    click.echo(f"pressure          {decision.pressure.value}")
    click.echo(f"batch size        {decision.batch_size}")
    click.echo(f"concurrency       {decision.concurrency}")
    click.echo(f"payload mode      {decision.payload_mode.value}")
    click.echo(f"recycle client    {decision.recycle_client}")
    if decision.halt_reason:
        click.echo(f"would halt        {decision.halt_reason}")
    for note in decision.notes:
        click.echo(f"note              {note}")


@main.command()
@click.argument("input_path", type=click.Path(path_type=Path))
def plan(input_path: Path) -> None:
    """Show the starting batch size and recycle threshold chosen for this workload."""
    cfg = Settings()
    ids = load_ids(input_path)
    _, available_mb = read_memory()
    start = cfg.batch_size or initial_batch_size(
        available_mb, cfg.min_batch_size, cfg.max_batch_size
    )
    governor = ResourceGovernor(limits_from_settings(cfg), start, len(ids))

    click.echo(f"items              {len(ids)}")
    click.echo(f"memory available   {available_mb:.0f}MB")
    click.echo(f"starting batch     {governor.batch_size}")
    click.echo(f"batch bounds       {cfg.min_batch_size}-{cfg.max_batch_size}")
    click.echo(f"recycle threshold  {governor.recycle_at_mb:.0f}MB process rss")
    click.echo(f"grow after         {cfg.growth_patience} consecutive low-memory checks")
    click.echo(f"halt if disk below {cfg.disk_halt_mb:.0f}MB")
