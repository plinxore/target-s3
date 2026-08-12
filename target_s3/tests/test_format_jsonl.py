"""Unit tests for FormatJsonl: compression and datetime/decimal serialization.

These tests use synthetic records only (no real tap/database dependency) and
mock S3 via moto, so they exercise the exact code path used in production
(FormatBase._write -> smart_open -> boto3 client) without needing a live
bucket.
"""

from __future__ import annotations

import datetime as dt
import decimal
import gzip
import logging
import re
import uuid

import boto3
import pytest
from moto import mock_s3

from target_s3.formats.format_jsonl import FormatJsonl

BUCKET = "test-bucket"


def make_config(**overrides) -> dict:
    config = {
        "format": {"format_type": "jsonl"},
        "cloud_provider": {
            "cloud_provider_type": "aws",
            "aws": {"aws_bucket": BUCKET, "aws_region": "us-east-1"},
        },
        "prefix": "myprefix",
        "append_date_to_prefix": False,
        "append_date_to_filename": False,
        "use_raw_stream_name": False,
        "stream_name_path_override": None,
    }
    config.update(overrides)
    return config


def make_context(records: list[dict], batch_number: int = 1) -> dict:
    return {
        "stream_name": "mystream",
        "logger": logging.getLogger("test"),
        "batch_start_time": dt.datetime.now(dt.timezone.utc),
        "batch_number": batch_number,
        "batch_uuid": uuid.uuid4(),
        "records": records,
    }


@pytest.fixture
def s3_client():
    with mock_s3():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def _run_and_fetch(s3_client, config: dict, records: list[dict]) -> bytes:
    fmt = FormatJsonl(config, make_context(records))
    fmt.run()
    key = fmt.fully_qualified_key.split(f"{BUCKET}/", 1)[1]
    return fmt.fully_qualified_key, s3_client.get_object(Bucket=BUCKET, Key=key)["Body"].read()


class TestCompression:
    def test_gzip_default_produces_a_real_gzip_file(self, s3_client, tmp_path):
        key, raw = _run_and_fetch(
            s3_client, make_config(), [{"id": 1}, {"id": 2}]
        )

        assert key.endswith(".jsonl.gz")

        # Prove it's real gzip, not just a renamed plaintext file: write it to
        # disk and run the actual `gzip -t` integrity check, then read the
        # decompressed content back.
        local_file = tmp_path / "out.jsonl.gz"
        local_file.write_bytes(raw)
        import subprocess

        result = subprocess.run(["gzip", "-t", str(local_file)], capture_output=True)
        assert result.returncode == 0, result.stderr

        content = gzip.decompress(raw).decode("utf-8")
        assert content == '{"id": 1}\n{"id": 2}'

    def test_compression_none_writes_plain_uncompressed_text(self, s3_client):
        key, raw = _run_and_fetch(
            s3_client, make_config(compression="none"), [{"id": 1}, {"id": 2}]
        )

        assert key.endswith(".jsonl")
        assert not key.endswith(".gz")
        assert raw[:2] != b"\x1f\x8b"
        assert raw.decode("utf-8") == '{"id": 1}\n{"id": 2}'

    def test_unknown_compression_value_is_rejected(self, s3_client):
        with pytest.raises(AssertionError):
            FormatJsonl(make_config(compression="bzip2"), make_context([{"id": 1}]))


class TestFilename:
    def test_falls_back_to_stream_name_when_no_date_appended(self, s3_client):
        # append_date_to_filename/prefix are both False in make_config(): with
        # nothing to build a basename from, the key used to end in a bare
        # ".jsonl.gz" (no filename, just the extension) -- confirmed while
        # running the phase 5/6 fixture tests with this same config shape.
        key, _ = _run_and_fetch(s3_client, make_config(), [{"id": 1}])
        basename = key.split("/")[-1]
        assert re.match(r"^mystream-part-00001-[0-9a-f-]{36}\.jsonl\.gz$", basename), basename

    def test_single_segment_stream_name_path_override_matches_no_override_shape(
        self, s3_client
    ):
        # Non-regression baseline: a single-segment override (no "/") behaves
        # exactly like falling back to stream_name -- both the folder and the
        # filename are just that one segment, same as the no-override case
        # above (just with "invoices" standing in for the real stream name).
        key, _ = _run_and_fetch(
            s3_client, make_config(stream_name_path_override="invoices"), [{"id": 1}]
        )
        assert key.split("/")[:-1] == [BUCKET, "myprefix", "invoices"], key
        basename = key.split("/")[-1]
        assert re.match(r"^invoices-part-00001-[0-9a-f-]{36}\.jsonl\.gz$", basename), basename

    def test_multi_segment_stream_name_path_override_folder_keeps_all_segments_filename_keeps_only_the_last(
        self, s3_client
    ):
        # The bug this guards against: create_key() reused the full
        # stream_name_path_override for both the folder AND the filename.
        # For a single-segment override that's harmless (folder and filename
        # are identical), but a multi-segment override like "base/table"
        # embedded a literal "/" in the filename variable -- which, once
        # concatenated into the S3 key, functions as yet another folder
        # level. The key ended up looking like
        # ".../base/table/base/table-part-...", a duplicated, spurious
        # folder that broke ClickHouse's ".../day=*/*.jsonl.gz" glob (it
        # expects exactly one path segment under the day partition, not
        # two).
        key, _ = _run_and_fetch(
            s3_client,
            make_config(
                stream_name_path_override="base/table",
                append_date_to_prefix=True,
                append_date_to_prefix_grain="day",
                partition_name_enabled=True,
            ),
            [{"id": 1}],
        )

        # Folder keeps every segment of the override, in order, with the
        # date partition appended after all of them.
        folder = "/".join(key.split("/")[:-1])
        assert re.match(
            rf"^{re.escape(BUCKET)}/myprefix/base/table/year=\d{{4}}/month=\d{{2}}/day=\d{{2}}$",
            folder,
        ), folder

        # Filename uses only the last segment ("table"), not the full
        # override value -- no embedded "/", no duplicated "base".
        basename = key.split("/")[-1]
        assert re.match(r"^table-part-00001-[0-9a-f-]{36}\.jsonl\.gz$", basename), basename
        assert "base" not in basename
        assert "/" not in basename

        # The concrete regression check: exactly one file directly under the
        # day partition, matching ClickHouse's glob shape -- fnmatch is what
        # actually proves this, not just eyeballing the string.
        import fnmatch

        assert fnmatch.fnmatch(
            key, f"{BUCKET}/myprefix/base/table/year=*/month=*/day=*/*.jsonl.gz"
        )
        # "table" legitimately appears twice (last folder segment, and
        # the filename that reuses it) -- the actual regression signature
        # was "base" appearing a second time, duplicated into the
        # filename by the old bug. That must appear exactly once: only as
        # the folder segment it's meant to be.
        assert key.count("base") == 1


class TestSerialization:
    def test_datetime_field_is_serialized_as_iso_string(self, s3_client):
        record = {"id": 1, "created_at": dt.datetime(2024, 3, 5, 12, 30, 45)}
        _, raw = _run_and_fetch(s3_client, make_config(), [record])

        content = gzip.decompress(raw).decode("utf-8")
        assert content == '{"id": 1, "created_at": "2024-03-05T12:30:45"}'

    def test_decimal_field_preserves_exact_precision(self, s3_client):
        # Mirrors what singer_sdk hands the target: json.loads(..., parse_float=Decimal)
        # reconstructs a Decimal straight from the source JSON text.
        record = {"id": 1, "amount": decimal.Decimal("1234.560")}
        _, raw = _run_and_fetch(s3_client, make_config(), [record])

        content = gzip.decompress(raw).decode("utf-8")
        assert content == '{"id": 1, "amount": 1234.560}'
