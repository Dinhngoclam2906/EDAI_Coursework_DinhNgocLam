"""
Pipeline run metadata logger.
Writes run records to data/pipeline_runs.jsonl for monitoring.
"""

import json
import uuid
from datetime import datetime
from pathlib import Path


class RunLogger:
    def __init__(self, pipeline_name: str, output_dir: str = "data"):
        self.pipeline_name = pipeline_name
        self.run_id = str(uuid.uuid4())[:8]
        self.start_ts = datetime.utcnow().isoformat()
        self.output_path = Path(output_dir) / "pipeline_runs.jsonl"
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

    def finish(self, status: str, input_rows: int = 0, output_rows: int = 0, error_msg: str = "") -> None:
        record = {
            "run_id":        self.run_id,
            "pipeline":      self.pipeline_name,
            "start_ts":      self.start_ts,
            "end_ts":        datetime.utcnow().isoformat(),
            "status":        status,        # success | failed
            "input_rows":    input_rows,
            "output_rows":   output_rows,
            "error_msg":     error_msg,
        }
        with open(self.output_path, "a") as f:
            f.write(json.dumps(record) + "\n")
        print(f"[RUN] {self.pipeline_name} [{self.run_id}] -> {status}  in={input_rows:,}  out={output_rows:,}")
