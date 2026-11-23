import socket
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import psutil


@dataclass(frozen=True)
class ResourceSnapshot:
    memory_percent: float
    memory_available_mb: float
    process_rss_mb: float
    disk_free_mb: float
    latency_ms: float | None

    @property
    def latency_known(self) -> bool:
        return self.latency_ms is not None


def read_memory() -> tuple[float, float]:
    vm = psutil.virtual_memory()
    return vm.percent, vm.available / (1024**2)


def read_process_rss() -> float:
    return psutil.Process().memory_info().rss / (1024**2)


def read_disk_free(path: Path) -> float:
    target = path
    while not target.exists() and target != target.parent:
        target = target.parent
    return psutil.disk_usage(str(target)).free / (1024**2)


def probe_latency(url: str, timeout: float = 2.0) -> float | None:
    if not url:
        return None

    parsed = urlparse(url if "//" in url else f"//{url}", scheme="http")
    host = parsed.hostname
    if not host:
        return None
    port = parsed.port or (443 if parsed.scheme == "https" else 80)

    start = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            pass
    except OSError:
        return None
    return (time.perf_counter() - start) * 1000


def snapshot(disk_path: Path, probe_url: str = "") -> ResourceSnapshot:
    memory_percent, memory_available_mb = read_memory()
    return ResourceSnapshot(
        memory_percent=memory_percent,
        memory_available_mb=memory_available_mb,
        process_rss_mb=read_process_rss(),
        disk_free_mb=read_disk_free(disk_path),
        latency_ms=probe_latency(probe_url),
    )


def recycle_threshold_mb(total_items: int) -> float:
    if total_items <= 10:
        return 2000.0
    if total_items <= 50:
        return 1600.0
    if total_items <= 150:
        return 1200.0
    if total_items <= 300:
        return 1000.0
    return 800.0


def initial_batch_size(available_mb: float, minimum: int, maximum: int) -> int:
    if available_mb > 16 * 1024:
        candidate = 512
    elif available_mb > 8 * 1024:
        candidate = 256
    elif available_mb > 4 * 1024:
        candidate = 128
    elif available_mb > 2 * 1024:
        candidate = 64
    else:
        candidate = 32
    return max(minimum, min(candidate, maximum))
