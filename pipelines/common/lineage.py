"""
DataHub lineage publisher for FinGuard pipelines.
Gracefully skips if DataHub is not reachable.
"""

import json
import os
import urllib.request
from datetime import datetime


# Inside Docker: datahub-gms:8080 (internal network hostname)
# Local dev:     localhost:8085 (mapped external port)
DATAHUB_URL = os.environ.get("DATAHUB_GMS_URL", "http://localhost:8085")
PLATFORM = "finguard"


def _urn_dataset(table: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{PLATFORM},{table},PROD)"


def _urn_job(job_name: str) -> str:
    return f"urn:li:dataJob:(urn:li:dataFlow:(spark,finguard_pipeline,PROD),{job_name})"


def publish_lineage(job_name: str, inputs: list[str], outputs: list[str]) -> None:
    """
    Publishes dataset -> job -> dataset lineage to DataHub.
    Fails silently if DataHub is unavailable (acceptable in dev).
    """
    payload = {
        "proposal": {
            "entityType": "dataJob",
            "entityUrn": _urn_job(job_name),
            "changeType": "UPSERT",
            "aspectName": "dataJobInputOutput",
            "aspect": {
                "value": json.dumps({
                    "inputDatasets": [_urn_dataset(t) for t in inputs],
                    "outputDatasets": [_urn_dataset(t) for t in outputs],
                    "inputDatajobs": [],
                }),
                "contentType": "application/json",
            },
        }
    }

    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            f"{DATAHUB_URL}/aspects?action=ingestProposal",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            print(f"[LINEAGE] {job_name}: published to DataHub ({resp.status})")
    except Exception as exc:
        print(f"[LINEAGE] {job_name}: DataHub unavailable, skipping ({exc})")
