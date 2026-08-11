"""Permanent regression test for the OOM incident that motivated this fork.

target-s3-jsonl's default flush mechanism measured `sys.getsizeof()` of a
Python list (i.e. pointer/container size only), which never crosses its
threshold in practice, so a full ~1M-row stream (source_db.invoices)
was held entirely in memory before a single byte was written, and the pod
OOM-killed. singer_sdk's BatchSink flushes on a real per-record counter
(`max_batch_size`, default 10000) instead, which structurally bounds memory
-- but that bound is on record *count*, not bytes, so it only proves
anything on rows as wide as our real data. This test drives >=1M records
shaped like `invoices` (25 columns, several max-width strings) through the
real Target/Sink pipeline and asserts memory stays flat across batches
rather than growing with the number of records processed.

This is slow (several minutes) by design -- it is exercising real
end-to-end throughput, not a mock. Run explicitly with `pytest -m load`.
"""

from __future__ import annotations

import io
import json
import logging
import statistics
from pathlib import Path

import boto3
import psutil
import pytest
from moto import mock_s3

from target_s3 import sinks as sinks_mod
from target_s3.target import Targets3

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "invoices-fixture.singer.jsonl"
BUCKET = "test-bucket"
RECORD_COUNT = 1_000_000
MAX_BATCH_SIZE = 10_000

# How much the average RSS of the last quarter of batches is allowed to
# exceed the average RSS of the first quarter, once the process has warmed
# up. Real measured ratio on the reference run was ~1.1x; a proportional
# (unbounded) leak like the one this test guards against would show a
# ratio in the tens-to-hundreds, not a fraction over 1.0.
MAX_ACCEPTABLE_GROWTH_RATIO = 1.75


class _StreamFeeder(io.TextIOBase):
    """Yields a SCHEMA line followed by `n` RECORD lines, cycling through
    `base_records` and overwriting only `id`, without materializing the
    whole stream in memory."""

    def __init__(self, schema_line: str, base_records: list[dict], n: int) -> None:
        self.schema_line = schema_line
        self.base_records = base_records
        self.n = n
        self._sent_schema = False
        self._i = 0

    def __iter__(self):
        return self

    def __next__(self) -> str:
        if not self._sent_schema:
            self._sent_schema = True
            return self.schema_line
        if self._i >= self.n:
            raise StopIteration
        record = dict(self.base_records[self._i % len(self.base_records)])
        record["id"] = self._i
        self._i += 1
        return json.dumps({"type": "RECORD", "stream": "invoices", "record": record}) + "\n"


@pytest.mark.load
def test_memory_stays_bounded_across_one_million_wide_records():
    logging.getLogger("target-s3").setLevel(logging.ERROR)

    with FIXTURE_PATH.open() as f:
        schema_line = f.readline()
        base_records = [json.loads(line)["record"] for line in f]

    process = psutil.Process()
    samples: list[tuple[int, float]] = []  # (records_processed, rss_mb)

    original_process_batch = sinks_mod.s3Sink.process_batch

    def instrumented_process_batch(self, context):
        original_process_batch(self, context)
        samples.append(
            (self._total_records_read, process.memory_info().rss / (1024 * 1024))
        )
        # Each batch now gets its own unique key (the -part-NNNNN counter,
        # fixing the silent-overwrite bug -- see sinks.py/format_base.py).
        # moto's mock S3 is a pure in-process emulation: every object it
        # has ever received stays resident in *this test process's* RAM for
        # the life of the mock_s3() context. Against real S3/MinIO the
        # written bytes leave the target's process once uploaded and don't
        # accumulate there -- but left alone here, moto's own bookkeeping
        # would make ~100 x ~3.6MB of retained mock objects look like a
        # leak in target-s3's own code, which it isn't (confirmed: RSS grew
        # ~2x, matching batch_count * per_object_size almost exactly, and
        # target-s3 holds no references across batches -- start_drain()
        # dereferences the prior batch's record list before every write).
        # Deleting each batch's object right after sampling keeps this test
        # measuring the target's own footprint, not the mock's.
        objects = client.list_objects_v2(Bucket=BUCKET).get("Contents", [])
        for obj in objects:
            client.delete_object(Bucket=BUCKET, Key=obj["Key"])

    sinks_mod.s3Sink.process_batch = instrumented_process_batch
    try:
        config = {
            "format": {"format_type": "jsonl"},
            "compression": "gzip",
            "cloud_provider": {
                "cloud_provider_type": "aws",
                "aws": {"aws_bucket": BUCKET, "aws_region": "us-east-1"},
            },
            "prefix": "source_db",
            "append_date_to_prefix": False,
            "append_date_to_filename": False,
            "use_raw_stream_name": False,
            "max_batch_size": MAX_BATCH_SIZE,
        }

        with mock_s3():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket=BUCKET)
            target = Targets3(config=config)
            target.listen(_StreamFeeder(schema_line, base_records, RECORD_COUNT))
    finally:
        sinks_mod.s3Sink.process_batch = original_process_batch

    assert len(samples) >= 10, (
        f"expected multiple batch flushes for {RECORD_COUNT} records at "
        f"max_batch_size={MAX_BATCH_SIZE}, got {len(samples)} samples"
    )

    # Skip the first couple of batches: RSS ramps up during interpreter/lib
    # warmup (imports, first allocations) regardless of the sink's own
    # behavior, so judging "flat" from record 0 would be misleading.
    warmed_up = samples[2:]
    quarter = max(1, len(warmed_up) // 4)
    first_quarter_avg = statistics.mean(rss for _, rss in warmed_up[:quarter])
    last_quarter_avg = statistics.mean(rss for _, rss in warmed_up[-quarter:])

    growth_ratio = last_quarter_avg / first_quarter_avg
    assert growth_ratio <= MAX_ACCEPTABLE_GROWTH_RATIO, (
        f"RSS grew {growth_ratio:.2f}x from the first to the last quarter of "
        f"batches ({first_quarter_avg:.1f}MB -> {last_quarter_avg:.1f}MB) "
        f"while processing {RECORD_COUNT} records. Memory should stay flat "
        f"across batches (bounded by max_batch_size), not grow with the "
        f"total number of records processed -- this is exactly the failure "
        f"mode (target-s3-jsonl's sys.getsizeof() flush that never fires) "
        f"this fork exists to avoid."
    )
