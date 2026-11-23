#!/usr/bin/env bash
set -euo pipefail

BLUE='\033[0;34m'; GREEN='\033[0;32m'; YELLOW='\033[0;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${BLUE}==>${NC} $1"; }
ok()    { echo -e "${GREEN} ok ${NC} $1"; }
warn()  { echo -e "${YELLOW}warn${NC} $1"; }
fail()  { echo -e "${RED}fail${NC} $1"; exit 1; }

WITH_S3=0
WITH_EXCEL=0
for arg in "$@"; do
    case "$arg" in
        --s3)    WITH_S3=1 ;;
        --excel) WITH_EXCEL=1 ;;
        --all)   WITH_S3=1; WITH_EXCEL=1 ;;
        -h|--help)
            cat <<'USAGE'
Usage: bash setup.sh [options]

  --s3      also install boto3 for uploading results to S3
  --excel   also install openpyxl for .xlsx output
  --all     both
  -h        this message

With no options you get a working install with local JSON and CSV output.
USAGE
            exit 0 ;;
        *) fail "unknown option: $arg (try --help)" ;;
    esac
done

info "Checking Python"
if ! command -v python3 >/dev/null 2>&1; then
    echo "python3 was not found. Install it first:"
    echo "  macOS:         brew install python@3.12"
    echo "  Ubuntu/Debian: sudo apt install python3 python3-venv"
    echo "  RHEL/Fedora:   sudo dnf install python3"
    fail "python3 missing"
fi

PY_OK=$(python3 -c 'import sys; print(1 if sys.version_info >= (3, 11) else 0)')
PY_VER=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
if [ "$PY_OK" != "1" ]; then
    echo "Python 3.11 or newer is required. Found $PY_VER."
    echo "  macOS:         brew install python@3.12"
    echo "  Ubuntu/Debian: sudo add-apt-repository ppa:deadsnakes/ppa && sudo apt install python3.12 python3.12-venv"
    fail "python $PY_VER too old"
fi
ok "Python $PY_VER"

info "Creating virtual environment"
if [ ! -d .venv ]; then
    python3 -m venv .venv || fail "could not create .venv (on Debian/Ubuntu you may need: sudo apt install python3-venv)"
fi
ok ".venv ready"

EXTRAS="dev"
[ "$WITH_S3" = "1" ] && EXTRAS="$EXTRAS,s3"
[ "$WITH_EXCEL" = "1" ] && EXTRAS="$EXTRAS,excel"

info "Installing dependencies [$EXTRAS]"
.venv/bin/pip install --quiet --upgrade pip
.venv/bin/pip install --quiet -e ".[$EXTRAS]" || fail "pip install failed"
ok "dependencies installed"

info "Verifying imports"
.venv/bin/python - <<'PYCHECK' || fail "core imports failed"
import httpx, psutil, click            # noqa: F401
from pydantic_settings import BaseSettings  # noqa: F401
from adaptive_fetch.core import Fetcher, ResourceGovernor  # noqa: F401
from adaptive_fetch.sinks import get_sink  # noqa: F401
PYCHECK
ok "imports resolve"

info "Verifying resource sampling on this machine"
.venv/bin/python - <<'PYCHECK' || fail "resource sampling failed"
from pathlib import Path
from adaptive_fetch.core.resources import snapshot

snap = snapshot(Path("."))
assert snap.memory_percent > 0, "could not read system memory"
assert snap.memory_available_mb > 0, "could not read available memory"
assert snap.process_rss_mb > 0, "could not read process rss"
assert snap.disk_free_mb > 0, "could not read disk free space"
print(f"     memory {snap.memory_percent:.0f}% used, {snap.disk_free_mb:.0f}MB disk free")
PYCHECK
ok "psutil can read memory and disk"

info "End-to-end check: fetch, checkpoint, resume, write"
.venv/bin/python - <<'PYCHECK' || fail "end-to-end check failed"
import asyncio, json, shutil, tempfile
from pathlib import Path

from adaptive_fetch.config import Settings
from adaptive_fetch.core.checkpoint import Checkpoint
from adaptive_fetch.core.fetcher import Fetcher
from adaptive_fetch.core.resources import ResourceSnapshot
from adaptive_fetch.sinks import get_sink

work = Path(tempfile.mkdtemp(prefix="adaptive-fetch-verify-"))
try:
    ids = work / "in.csv"
    ids.write_text("id\n" + "\n".join(f"V-{i:03d}" for i in range(12)) + "\n")

    cfg = Settings(
        input_path=ids,
        output_path=work / "out",
        checkpoint_dir=work / ".ckpt",
        batch_size=5,
        checkpoint_every=1,
        retries=0,
    )

    async def fake(item_id, mode):
        return {"body": {"id": item_id}, "bytes": 10}

    healthy = ResourceSnapshot(50.0, 8192.0, 100.0, 50000.0, 100.0)
    report = asyncio.run(Fetcher(cfg, fake, lambda: healthy).run())
    assert report.fetched == 12, f"expected 12 fetched, got {report.fetched}"
    assert report.errors == 0, f"unexpected errors: {report.errors}"

    resumed = asyncio.run(Fetcher(cfg, fake, lambda: healthy).run())
    assert resumed.skipped == 12, f"resume did not skip: {resumed.skipped}"
    assert resumed.fetched == 0, "resume refetched records"

    records = Checkpoint(cfg.checkpoint_dir).records
    written = get_sink("local", cfg).write(records, report.as_summary())
    payload = json.loads((cfg.output_path / "results.json").read_text())
    assert len(payload["records"]) == 12, "sink wrote wrong record count"
    assert payload["summary"]["fetched"] == 12, "summary is wrong"

    low_disk = ResourceSnapshot(50.0, 8192.0, 100.0, 10.0, 100.0)
    shutil.rmtree(cfg.checkpoint_dir)
    halted = asyncio.run(Fetcher(cfg, fake, lambda: low_disk).run())
    assert halted.halted_reason is not None, "low disk did not halt the run"

    print(f"     fetched 12, resumed clean, wrote {len(written)} files, halt guard works")
finally:
    shutil.rmtree(work, ignore_errors=True)
PYCHECK
ok "end-to-end verified"

info "Running the test suite"
.venv/bin/python -m pytest -q || fail "tests failed"
ok "tests pass"

if [ "$WITH_S3" = "1" ]; then
    info "Checking AWS credentials for the s3 sink"
    if .venv/bin/python -c "import boto3; boto3.client('sts').get_caller_identity()" >/dev/null 2>&1; then
        ok "AWS credentials resolve"
    else
        warn "boto3 installed but no usable AWS credentials found."
        warn "  Configure with: aws configure   (or set AWS_PROFILE / AWS_ACCESS_KEY_ID)"
        warn "  The local sink works without this."
    fi
fi

cat <<'DONE'

Setup complete.

  source .venv/bin/activate

  adaptive-fetch probe                        # what does this machine look like right now
  adaptive-fetch plan data/sample_input.csv   # sizing chosen for this workload
  adaptive-fetch run data/sample_input.csv --base-url https://api.example.com

Governor decisions log at WARNING, so they show by default.
DONE
