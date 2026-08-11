"""Phase 8 (mocked portion) -- end-to-end integration validation.

This is as far as integration validation can go without real infrastructure:
everything here runs against `moto`'s in-memory S3 mock, using synthetic
Singer streams only. It proves the target's own logic (path building,
compression, serialization, multi-stream/multi-batch bookkeeping) is
correct. It does NOT and CANNOT prove:

  - That writes succeed against a real S3-compatible endpoint (MinIO) with
    real network/auth/TLS behavior.
  - That `aws_endpoint_override` (the config knob for pointing at MinIO
    instead of AWS) actually works: moto does not reliably intercept
    requests once a custom `endpoint_url` is passed to the boto3 client --
    an earlier version of this test set aws_endpoint_override to a
    made-up host and it hung for two minutes attempting a real DNS
    lookup/connection instead of hitting the mock. Tests here omit
    aws_endpoint_override entirely as a result; MinIO endpoint wiring is
    exactly the kind of thing that needs a real endpoint to validate.
  - That real AWS/MinIO credentials work end to end.
  - That ClickHouse's s3() table function can actually read the files this
    target produces (schema inference, gzip handling on the ClickHouse
    side, partition pruning on the `year=/month=/day=` layout).
  - Real-world throughput/latency, multipart upload edge cases, or MinIO's
    actual consistency/list-objects behavior under concurrent writers
    (moto is a simplified, single-process emulation).

Those are explicitly left to validation against real MinIO, run separately
by whichever environment has that infrastructure.
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import re
from pathlib import Path

import boto3
import pytest
from moto import mock_s3

from target_s3.target import Targets3

FIXTURES = Path(__file__).parent / "fixtures"
INVOICES_FIXTURE = FIXTURES / "invoices-fixture.singer.jsonl"
VILLES_FIXTURE = FIXTURES / "villes-fixture.singer.jsonl"
BUCKET = "bronze"

# Mirrors our real path convention (year=/month=/day=, Hive style) -- this
# is the same layout the original architecture doc says is already used in
# production for source_db, and it is what target-s3 actually supports.
# It does NOT produce a single combined "dt=YYYY-MM-DD" folder. Filename
# uniqueness comes from a monotonic per-run "-part-NNNNN" counter, not a
# timestamp (a wall-clock suffix can collide under load and breaks
# idempotency across reruns -- see TestMultiBatchSameStream/TestIdempotency).
KEY_PATTERN = re.compile(
    r"^sources/source_db/(?P<stream>\w+)/"
    r"year=(?P<year>\d{4})/month=(?P<month>\d{2})/day=(?P<day>\d{2})/"
    r"(?P=stream)-part-(?P<part>\d{5})\.jsonl\.gz$"
)


def base_config(**overrides) -> dict:
    config = {
        "format": {"format_type": "jsonl"},
        "compression": "gzip",
        "datetime_error_treatment": "null",
        "cloud_provider": {
            "cloud_provider_type": "aws",
            "aws": {
                "aws_bucket": BUCKET,
                "aws_region": "us-east-1",
            },
        },
        "prefix": "sources/source_db",
        "append_date_to_prefix": True,
        "partition_name_enabled": True,
        "append_date_to_prefix_grain": "day",
        # Uniqueness now comes from the part-counter, not a timestamp -- no
        # need to append a date/time string to the filename by default.
        "append_date_to_filename": False,
        "max_batch_size": 10000,
    }
    config.update(overrides)
    return config


@pytest.fixture
def s3_client():
    with mock_s3():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def _listen(config: dict, stream_content: str) -> None:
    logging.getLogger("target-s3").setLevel(logging.ERROR)
    target = Targets3(config=config)
    target.listen(io.StringIO(stream_content))


def _all_objects(client) -> dict[str, bytes]:
    objs = client.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    return {o["Key"]: client.get_object(Bucket=BUCKET, Key=o["Key"])["Body"].read() for o in objs}


def _decompress_lines(raw: bytes) -> list[dict]:
    return [json.loads(l) for l in gzip.decompress(raw).decode("utf-8").split("\n") if l]


class TestMultiStream:
    """Two streams of very different shape (25-column invoices vs 6-column
    villes) processed in the same run -- proves the sink-per-stream
    bookkeeping doesn't cross-contaminate paths or content."""

    def test_two_streams_produce_two_correct_and_separate_paths(self, s3_client):
        # Interleave: invoices SCHEMA + a slice of records, then villes
        # SCHEMA + all its records, then the rest of invoices -- a tap
        # emitting multiple streams rarely does one stream fully before
        # starting the next.
        with INVOICES_FIXTURE.open() as f:
            invoices_lines = f.read().splitlines()
        with VILLES_FIXTURE.open() as f:
            villes_lines = f.read().splitlines()

        interleaved = (
            [invoices_lines[0]]  # invoices SCHEMA
            + invoices_lines[1:101]  # first 100 invoices records
            + villes_lines  # villes SCHEMA + all 30 records
            + invoices_lines[101:]  # remaining invoices records
        )
        _listen(base_config(), "\n".join(interleaved) + "\n")

        objects = _all_objects(s3_client)
        assert len(objects) == 2, f"expected exactly 2 objects, got {list(objects)}"

        by_stream = {}
        for key in objects:
            m = KEY_PATTERN.match(key)
            assert m, f"key does not match expected path shape: {key}"
            by_stream[m.group("stream")] = key

        assert set(by_stream) == {"invoices", "villes"}

        invoices_records = _decompress_lines(objects[by_stream["invoices"]])
        villes_records = _decompress_lines(objects[by_stream["villes"]])
        assert len(invoices_records) == 405
        assert len(villes_records) == 30
        # no cross-contamination: villes-only/invoices-only fields never leak
        # across streams (invoices also happens to have its own "ville"
        # address column, so "population"/"amount" are the fields that
        # actually distinguish the two schemas)
        assert all("population" not in r for r in invoices_records)
        assert all("amount" not in r for r in villes_records)


class TestRealPathShape:
    def test_key_matches_bronze_sources_base_table_partitioned_path(self, s3_client):
        with INVOICES_FIXTURE.open() as f:
            content = f.read()
        _listen(base_config(), content)

        [key] = _all_objects(s3_client).keys()
        m = KEY_PATTERN.match(key)
        assert m, f"key does not match expected path shape: {key}"
        assert m.group("stream") == "invoices"
        assert m.group("part") == "00001"
        # fix #38 (empty filename -> stream_name fallback) holds even with
        # the part-counter appended: the filename still *starts* with the
        # stream name, matching .../invoices-part-00001.jsonl.gz exactly.
        assert key.split("/")[-1] == "invoices-part-00001.jsonl.gz"


class TestMultiBatchSameStream:
    """max_batch_size=150 against the 405-record invoices fixture forces 3
    batches for a single stream -- the real risk surface for large tables
    like the production `invoices` (~1M rows / 10k per batch ~= 178
    batches). These were "test_KNOWN_GOTCHA_..." before the -part-NNNNN
    counter existed: every batch for a stream on the same UTC day computed
    the identical key, so batch N silently overwrote batch N-1's object --
    the run reported success, nothing raised, and only the last batch
    survived. The counter fixes this at the root; these are now the
    permanent non-regression guard for it."""

    def test_all_batches_are_kept_via_part_counter(self, s3_client):
        with INVOICES_FIXTURE.open() as f:
            content = f.read()
        _listen(base_config(max_batch_size=150), content)

        objects = _all_objects(s3_client)
        assert len(objects) == 3, (
            f"expected 3 distinct objects (one per batch), got {len(objects)}: "
            f"{list(objects)}"
        )
        assert {KEY_PATTERN.match(k).group("part") for k in objects} == {
            "00001",
            "00002",
            "00003",
        }
        all_ids = set()
        total = 0
        for raw in objects.values():
            records = _decompress_lines(raw)
            total += len(records)
            all_ids.update(r["id"] for r in records)
        assert total == 405, f"expected 405 total records across all batches, got {total}"
        assert all_ids == set(range(1, 406)), "some ids missing or duplicated across batches"

    def test_no_overwrite_even_with_the_historically_dangerous_day_grain(self, s3_client):
        # Belt and suspenders: even if append_date_to_filename is turned
        # back on with the target's own schema-default grain ("day" --
        # exactly the setting that used to cause silent data loss), the
        # part-counter alone is what guarantees uniqueness now, so the
        # outcome must be identical to the counter-only case above.
        with INVOICES_FIXTURE.open() as f:
            content = f.read()
        _listen(
            base_config(
                max_batch_size=150,
                append_date_to_filename=True,
                append_date_to_filename_grain="day",
            ),
            content,
        )

        objects = _all_objects(s3_client)
        assert len(objects) == 3, (
            f"expected 3 distinct objects even with day-grain filenames, got "
            f"{len(objects)}: {list(objects)}"
        )
        total = sum(len(_decompress_lines(raw)) for raw in objects.values())
        assert total == 405

    def test_large_batch_count_matching_real_invoices_ratio(self, s3_client):
        # source_db.invoices is ~~1M rows at max_batch_size=10000,
        # i.e. ~178 batches. Reproducing that record volume here would be
        # slow for a default (non "load"-marked) test, so this keeps the
        # same *batch count* by cycling a handful of villes records with a
        # small max_batch_size -- what's under test is key uniqueness
        # across many batches, not throughput (that's test_load_memory.py).
        with VILLES_FIXTURE.open() as f:
            schema_line, *record_lines = f.read().splitlines()

        num_batches = 178
        batch_size = 3
        total_records = num_batches * batch_size

        def feed():
            yield schema_line + "\n"
            base_records = [json.loads(l)["record"] for l in record_lines]
            for i in range(total_records):
                record = dict(base_records[i % len(base_records)])
                record["id"] = i
                yield json.dumps({"type": "RECORD", "stream": "villes", "record": record}) + "\n"

        _listen(base_config(max_batch_size=batch_size), "".join(feed()))

        objects = _all_objects(s3_client)
        assert len(objects) == num_batches, (
            f"expected {num_batches} distinct objects, got {len(objects)}"
        )
        parts = {KEY_PATTERN.match(k).group("part") for k in objects}
        assert parts == {f"{n:05d}" for n in range(1, num_batches + 1)}

        all_ids = set()
        for raw in objects.values():
            all_ids.update(r["id"] for r in _decompress_lines(raw))
        assert all_ids == set(range(total_records)), "no batch's records were lost or duplicated"


class TestIdempotency:
    def test_rerunning_the_same_stream_overwrites_rather_than_duplicates(self, s3_client):
        # A retried/rerun extraction (e.g. a Dagster retry) replays the
        # exact same Singer stream. Batch N must get the exact same key on
        # both runs -- otherwise a rerun silently accumulates duplicate
        # objects that a downstream reader (ClickHouse s3()) would
        # double-count, which is exactly what a timestamp-based suffix
        # would do (new wall-clock time -> new filename -> new object).
        with INVOICES_FIXTURE.open() as f:
            content = f.read()
        config = base_config(max_batch_size=150)

        _listen(config, content)
        objects_after_first_run = _all_objects(s3_client)
        assert len(objects_after_first_run) == 3

        _listen(config, content)
        objects_after_second_run = _all_objects(s3_client)

        assert set(objects_after_second_run) == set(objects_after_first_run), (
            "a rerun produced different keys than the first run -- this would "
            "accumulate duplicates in MinIO instead of overwriting"
        )
        assert len(objects_after_second_run) == 3

        total = sum(len(_decompress_lines(raw)) for raw in objects_after_second_run.values())
        assert total == 405, "rerun should still total 405 records, not 810"


class TestCompressionEndToEnd:
    @pytest.mark.parametrize("compression", ["gzip", "none"])
    def test_compression_toggle_holds_in_full_pipeline(self, s3_client, tmp_path, compression):
        with INVOICES_FIXTURE.open() as f:
            content = f.read()
        _listen(base_config(compression=compression), content)

        [(key, raw)] = _all_objects(s3_client).items()
        if compression == "gzip":
            assert key.endswith(".jsonl.gz")
            local = tmp_path / "out.gz"
            local.write_bytes(raw)
            import subprocess

            result = subprocess.run(["gzip", "-t", str(local)], capture_output=True)
            assert result.returncode == 0, result.stderr
            records = _decompress_lines(raw)
        else:
            assert key.endswith(".jsonl")
            assert not key.endswith(".gz")
            assert raw[:2] != b"\x1f\x8b"
            records = [json.loads(l) for l in raw.decode("utf-8").split("\n") if l]

        assert len(records) == 405


class TestDatetimeErrorTreatmentInContext:
    def test_zero_dates_nulled_in_full_multi_stream_run(self, s3_client):
        with INVOICES_FIXTURE.open() as f:
            invoices_content = f.read()
        with VILLES_FIXTURE.open() as f:
            villes_content = f.read()

        _listen(base_config(), invoices_content + villes_content)

        objects = _all_objects(s3_client)
        invoices_key = next(k for k in objects if "/invoices/" in k)
        villes_key = next(k for k in objects if "/villes/" in k)

        invoices_records = {r["id"]: r for r in _decompress_lines(objects[invoices_key])}
        villes_records = {r["id"]: r for r in _decompress_lines(objects[villes_key])}

        # invoices: known zero-date/sentinel ids from the fixture (see
        # test_invoices_fixture.py for the full derivation)
        assert invoices_records[37]["date"] is None
        assert invoices_records[74]["date"] is None
        # villes: id 15 has a MyISAM zero-date "maj" by construction
        assert villes_records[15]["maj"] is None
