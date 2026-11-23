import json
from datetime import datetime, timezone
from typing import Any

from .base import Sink


class S3Sink(Sink):
    name = "s3"

    def __init__(self, bucket: str, prefix: str = "ballast", region: str = "us-east-1"):
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.region = region
        self._client: Any = None

    def _get_client(self) -> Any:
        if self._client is None:
            import boto3

            self._client = boto3.client("s3", region_name=self.region)
        return self._client

    def is_available(self) -> bool:
        if not self.bucket:
            return False
        try:
            self._get_client().head_bucket(Bucket=self.bucket)
        except Exception:
            return False
        return True

    def write(self, records: list[dict[str, Any]], summary: dict[str, Any]) -> list[str]:
        if not self.bucket:
            raise ValueError("s3 sink needs a bucket. Set AF_S3_BUCKET or pass --s3-bucket")

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        base = f"{self.prefix}/{stamp}" if self.prefix else stamp
        client = self._get_client()
        written: list[str] = []

        for key, body in (
            (f"{base}/results.json", {"summary": summary, "records": records}),
            (f"{base}/summary.json", summary),
        ):
            client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(body, indent=2, default=str).encode("utf-8"),
                ContentType="application/json",
            )
            written.append(f"s3://{self.bucket}/{key}")

        return written
