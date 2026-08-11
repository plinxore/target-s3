"""Replays a realistic Singer stream (structure of source_db.invoices,
content fully synthetic) end to end through the real Target/Sink pipeline.

This is the empirical validation that singer_sdk's native decimal handling
and datetime coercion behave correctly on data shaped like our actual
production table: 25 columns, 5 DECIMAL columns with `multipleOf` and no
`minimum`/`maximum` (plus one contrast column without `multipleOf`),
MyISAM zero-dates, NULL/all-NULL rows, and max-width strings.
"""

from __future__ import annotations

import decimal
import gzip
import io
import json
import logging
from pathlib import Path

import boto3
import pytest
from moto import mock_s3

from target_s3.target import Targets3

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "invoices-fixture.singer.jsonl"
BUCKET = "test-bucket"
DECIMAL_COLUMNS = [
    "amount",
    "disbursement",
    "tax_amount",
    "option_a",
    "option_b",
    "amount_ht",  # contrast column: no multipleOf
]


def make_config(**overrides) -> dict:
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
    }
    config.update(overrides)
    return config


@pytest.fixture
def s3_client():
    with mock_s3():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def _source_records() -> dict[int, dict]:
    records = {}
    with FIXTURE_PATH.open() as f:
        f.readline()  # SCHEMA line
        for line in f:
            d = json.loads(line, parse_float=decimal.Decimal)
            records[d["record"]["id"]] = d["record"]
    return records


def _run_fixture(s3_client, config: dict) -> tuple[dict, bytes]:
    logging.getLogger("target-s3").setLevel(logging.ERROR)
    target = Targets3(config=config)
    with FIXTURE_PATH.open() as f:
        content = f.read()
    target.listen(io.StringIO(content))

    objs = s3_client.list_objects_v2(Bucket=BUCKET)
    keys = [o["Key"] for o in objs.get("Contents", [])]
    assert len(keys) == 1, f"expected exactly one output object, got {keys}"
    raw = s3_client.get_object(Bucket=BUCKET, Key=keys[0])["Body"].read()
    return keys[0], raw


class TestInvoicesFixture:
    def test_replays_without_crashing(self, s3_client):
        # Historically this crashed: singer_sdk's default datetime error
        # treatment raises on unparseable date-time strings, and this
        # fixture contains real MyISAM zero-dates ("0000-00-00 00:00:00")
        # and a "__NULL__" sentinel. s3Sink.datetime_error_treatment must
        # be configured to tolerate these.
        _run_fixture(s3_client, make_config())

    def test_output_is_valid_gzip(self, s3_client, tmp_path):
        key, raw = _run_fixture(s3_client, make_config())
        assert key.endswith(".jsonl.gz")

        local_file = tmp_path / "out.jsonl.gz"
        local_file.write_bytes(raw)
        import subprocess

        result = subprocess.run(["gzip", "-t", str(local_file)], capture_output=True)
        assert result.returncode == 0, result.stderr

    def test_every_line_is_valid_jsonl_and_row_count_matches(self, s3_client):
        source = _source_records()
        _, raw = _run_fixture(s3_client, make_config())
        lines = [l for l in gzip.decompress(raw).decode("utf-8").split("\n") if l]

        assert len(lines) == len(source)
        for line in lines:
            json.loads(line)  # raises if invalid

    def test_decimal_columns_preserve_exact_precision(self, s3_client):
        source = _source_records()
        _, raw = _run_fixture(s3_client, make_config())
        output = {
            r["id"]: r
            for r in (
                json.loads(l, parse_float=decimal.Decimal)
                for l in gzip.decompress(raw).decode("utf-8").split("\n")
                if l
            )
        }

        checked = 0
        for record_id, src in source.items():
            out = output[record_id]
            for col in DECIMAL_COLUMNS:
                src_val, out_val = src.get(col), out.get(col)
                checked += 1
                if src_val is None:
                    assert out_val is None, f"id={record_id} col={col}"
                else:
                    assert src_val == out_val, (
                        f"id={record_id} col={col}: {src_val!r} != {out_val!r}"
                    )
        assert checked == len(source) * len(DECIMAL_COLUMNS)

    def test_unparseable_dates_are_nulled_not_crashed(self, s3_client):
        source = _source_records()
        expected_nulled_ids = {
            rid
            for rid, r in source.items()
            if r.get("date") in ("__NULL__", "0000-00-00 00:00:00")
        }
        assert expected_nulled_ids, "fixture should contain zero-date/sentinel cases"

        _, raw = _run_fixture(s3_client, make_config())
        output = {
            r["id"]: r
            for r in (json.loads(l) for l in gzip.decompress(raw).decode("utf-8").split("\n") if l)
        }

        for record_id in expected_nulled_ids:
            assert output[record_id]["date"] is None, record_id

    def test_all_null_rows_survive(self, s3_client):
        source = _source_records()
        all_null_ids = {
            rid
            for rid, r in source.items()
            if all(v is None for k, v in r.items() if k != "id")
        }
        assert all_null_ids, "fixture should contain all-NULL rows"

        _, raw = _run_fixture(s3_client, make_config())
        output = {
            r["id"]: r
            for r in (json.loads(l) for l in gzip.decompress(raw).decode("utf-8").split("\n") if l)
        }
        for record_id in all_null_ids:
            out = output[record_id]
            assert all(v is None for k, v in out.items() if k != "id"), record_id
