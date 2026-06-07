"""
Watermark utility — tracks the last successfully processed high-water mark per pipeline.

Stored as JSON in data/watermarks/<name>.json.
Pipelines read the watermark before processing and update it after a successful write.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

WATERMARK_DIR = Path("data/watermarks")


def get_watermark(name: str, default: str = "1970-01-01T00:00:00") -> str:
    path = WATERMARK_DIR / f"{name}.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8-sig"))["last_processed"]
    return default


def set_watermark(name: str, value: str) -> None:
    WATERMARK_DIR.mkdir(parents=True, exist_ok=True)
    path = WATERMARK_DIR / f"{name}.json"
    path.write_text(json.dumps({
        "last_processed": value,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }, indent=2), encoding="utf-8")
