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
# is the same layout already used in production for a sibling source
# table, and it is what target-s3 actually supports.
# It does NOT produce a single combined "dt=YYYY-MM-DD" folder. Filename
# uniqueness is "-part-NNNNN-<uuid>": the counter gives batches a readable
# order within a run, the UUID guarantees no two runs can ever collide on
# the same key (a same-day rerun gets a brand new UUID per batch, so it
# can never silently clobber a previous run's objects -- see
# TestMultiBatchSameStream/TestIdempotency below).
KEY_PATTERN = re.compile(
    r"^sources/source_db/(?P<stream>\w+)/"
    r"year=(?P<year>\d{4})/month=(?P<month>\d{2})/day=(?P<day>\d{2})/"
    r"(?P=stream)-part-(?P<part>\d{5})-"
    r"(?P<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
    r"\.jsonl\.gz$"
)

# Same shape as KEY_PATTERN but for append_uuid: false -- no UUID suffix, so
# keys are reusable across runs with the same batch layout.
KEY_PATTERN_NO_UUID = re.compile(
    r"^sources/source_db/(?P<stream>\w+)/"
    r"year=(?P<year>\d{4})/month=(?P<month>\d{2})/day=(?P<day>\d{2})/"
    r"(?P=stream)-part-(?P<part>\d{5})"
    r"\.jsonl\.gz$"
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
        # across streams (invoices also happens to have its own "city"
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
        # the part-counter/uuid appended: the filename still *starts* with
        # the stream name, matching .../invoices-part-00001-<uuid>.jsonl.gz.
        assert key.split("/")[-1].startswith("invoices-part-00001-")


class TestMultiBatchSameStream:
    """max_batch_size=150 against the 405-record invoices fixture forces 3
    batches for a single stream -- the real risk surface for large tables
    like our production invoicing table (~1M rows / 10k per batch ~= ~100
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

    def test_large_batch_count_matching_real_table_ratio(self, s3_client):
        # Our production invoicing table is ~1M rows at max_batch_size=10000,
        # i.e. ~100 batches. Reproducing that record volume here would be
        # slow for a default (non "load"-marked) test, so this keeps the
        # same *batch count* by cycling a handful of villes records with a
        # small max_batch_size -- what's under test is key uniqueness
        # across many batches, not throughput (that's test_load_memory.py).
        with VILLES_FIXTURE.open() as f:
            schema_line, *record_lines = f.read().splitlines()

        num_batches = 100
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
    """"Idempotency" here means never colliding, not never repeating: this
    target guarantees a rerun can never silently clobber a previous run's
    objects (that was the original bug, just moved from "between batches"
    to "between runs" if the part-counter had been the only mechanism).
    Whether a rerun should *replace* or *add to* what's already on disk is
    not this target's decision -- it's the consumer's: a full-refresh
    table reads a fixed path each run (so the pipeline points that path at
    a location it clears/replaces between runs); an incremental table
    reads a glob over the date partition and is expected to accumulate
    every run's files. This target only promises the second case is safe
    by construction -- it never has to guess which one a given table
    wants."""

    def test_rerunning_the_same_stream_accumulates_without_colliding(self, s3_client):
        # A retried/rerun extraction (e.g. a Dagster retry) replays the
        # exact same Singer stream. The counter alone would restart at 1
        # on every run and collide with the previous run's files; the UUID
        # is what makes that impossible regardless of how many times the
        # same stream is replayed on the same day.
        with INVOICES_FIXTURE.open() as f:
            content = f.read()
        config = base_config(max_batch_size=150)

        _listen(config, content)
        objects_after_first_run = _all_objects(s3_client)
        assert len(objects_after_first_run) == 3

        _listen(config, content)
        objects_after_both_runs = _all_objects(s3_client)

        assert len(objects_after_both_runs) == 6, (
            "expected the second run's 3 objects to accumulate alongside "
            "the first run's 3, not collide with or replace them"
        )
        second_run_keys = set(objects_after_both_runs) - set(objects_after_first_run)
        assert len(second_run_keys) == 3, (
            "the second run must not reuse any key from the first run"
        )
        for key in objects_after_both_runs:
            m = KEY_PATTERN.match(key)
            assert m, f"key does not match expected path shape: {key}"

        total = sum(len(_decompress_lines(raw)) for raw in objects_after_both_runs.values())
        assert total == 810, "both runs' records should all be present: 2 x 405"


class TestAppendUuidOff:
    """append_uuid: false trades the TestIdempotency guarantee (a rerun can
    never clobber a previous run) for the opposite: a rerun with the same
    batch layout reuses the same keys and overwrites in place. This is only
    the naming half of a real full-refresh overwrite -- see the second test
    for the trap that combining it with nothing else falls into, and the
    README's "Object key uniqueness & idempotency" section for the other two
    pieces (prefix purge, fixed read path) a pipeline still has to supply."""

    def test_append_uuid_false_produces_reusable_names_across_reruns(self, s3_client):
        with INVOICES_FIXTURE.open() as f:
            content = f.read()
        config = base_config(max_batch_size=150, append_uuid=False)

        _listen(config, content)
        objects_after_first_run = _all_objects(s3_client)
        assert len(objects_after_first_run) == 3
        for key in objects_after_first_run:
            m = KEY_PATTERN_NO_UUID.match(key)
            assert m, f"key does not match the no-UUID path shape: {key}"

        _listen(config, content)
        objects_after_second_run = _all_objects(s3_client)

        assert objects_after_second_run.keys() == objects_after_first_run.keys(), (
            "a second run with the same batch layout must overwrite the "
            "same keys, not accumulate new ones"
        )
        total = sum(len(_decompress_lines(raw)) for raw in objects_after_second_run.values())
        assert total == 405, (
            "overwritten objects must hold exactly one run's worth of "
            "records, not both runs' -- 810 here would mean this silently "
            "fell back to accumulating instead of overwriting"
        )

    def test_KNOWN_smaller_rerun_leaves_orphans_without_prefix_purge(self, s3_client):
        # This test does not "fix" the behavior it documents -- it proves
        # append_uuid: false alone is not a full-refresh mechanism. A first
        # run with a small max_batch_size writes 3 parts; a second,
        # differently-shaped run (large max_batch_size, so it fits in 1
        # batch) only overwrites part-00001. Nothing in this target purges
        # or even knows about part-00002/part-00003 from the first run --
        # that's explicitly the orchestration layer's job (purge the key
        # prefix before the run), not this target's. Skipping that step
        # leaves the smaller run's "full refresh" silently incomplete: a
        # downstream reader globbing the prefix still sees the orphaned
        # rows from run 1 alongside run 2's data.
        with INVOICES_FIXTURE.open() as f:
            content = f.read()

        _listen(base_config(max_batch_size=150, append_uuid=False), content)
        objects_after_first_run = _all_objects(s3_client)
        assert len(objects_after_first_run) == 3
        assert {KEY_PATTERN_NO_UUID.match(k).group("part") for k in objects_after_first_run} == {
            "00001",
            "00002",
            "00003",
        }
        # part-00002/part-00003's content, captured before run 2, is what
        # the orphan check below proves survives untouched.
        orphaned_content_from_first_run = {
            k: v for k, v in objects_after_first_run.items()
            if KEY_PATTERN_NO_UUID.match(k).group("part") != "00001"
        }

        _listen(base_config(max_batch_size=10000, append_uuid=False), content)
        objects_after_second_run = _all_objects(s3_client)

        assert len(objects_after_second_run) == 3, (
            "KNOWN gap: without a prefix purge between runs, part-00002 and "
            "part-00003 from the larger first run survive as orphans "
            "alongside the smaller second run's part-00001 -- append_uuid: "
            "false only makes keys reusable, it does not purge what a "
            "smaller rerun no longer writes"
        )
        assert {KEY_PATTERN_NO_UUID.match(k).group("part") for k in objects_after_second_run} == {
            "00001",
            "00002",
            "00003",
        }
        for key, raw in orphaned_content_from_first_run.items():
            assert objects_after_second_run[key] == raw, (
                f"{key} is an orphan from run 1 that run 2 never wrote -- "
                "it must be untouched, byte for byte"
            )
        [part_00001_key] = [
            k for k in objects_after_second_run
            if KEY_PATTERN_NO_UUID.match(k).group("part") == "00001"
        ]
        assert len(_decompress_lines(objects_after_second_run[part_00001_key])) == 405, (
            "part-00001 was overwritten by run 2's single batch, which "
            "holds the whole 405-record fixture"
        )


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
