"""Parquet schema stability across batches, and internal compression codec.

The bug this guards against: pyarrow.Table.from_pydict() with no explicit
schema infers types from whatever values are actually present in that one
batch. A column that happens to be all-null in one batch infers as
pyarrow's null() type; the same column with real values in a later batch of
the same stream infers a real type (e.g. double). Two Parquet files for the
same stream end up with genuinely different schemas and can't be read
together by a single reader -- confirmed directly against pyarrow before
writing this test:

    >>> pa.Table.from_pydict({"amount": [None, None]}).schema
    amount: null
    >>> pa.Table.from_pydict({"amount": [12.34, -56.78]}).schema
    amount: double
    >>> pa.concat_tables([...])
    ArrowInvalid: Schema at index 1 was different

create_schema() derives the schema once from the stream's Singer SCHEMA
message (declared types, not observed values) and is now the default path
(format_parquet.get_schema_from_tap defaults to true) -- every batch of a
stream gets the identical schema regardless of which columns happen to be
populated in that particular batch.

These tests write real Parquet files to the local filesystem (via a
FormatParquet subclass that swaps pyarrow's S3FileSystem for a
LocalFileSystem -- moto cannot mock pyarrow's own S3 client, which talks to
AWS through arrow's C++ SDK rather than botocore) and read them back with
plain pyarrow, matching how a real downstream reader (e.g. ClickHouse's
s3(..., 'Parquet', ...) over multiple files) would need to.
"""

from __future__ import annotations

import datetime as dt
import io
import json
import logging
from pathlib import Path

import pyarrow
import pyarrow.fs
import pyarrow.parquet
import pytest

from target_s3 import sinks as sinks_mod
from target_s3.formats import format_parquet as format_parquet_mod
from target_s3.target import Targets3

STREAM_SCHEMA = {
    "type": "SCHEMA",
    "stream": "invoices",
    "key_properties": ["id"],
    "schema": {
        "properties": {
            "id": {"type": ["null", "integer"]},
            "name": {"type": ["null", "string"]},
            # Mirrors our real case: a DECIMAL-shaped numeric column with no
            # bounds, which can legitimately be all-null in one batch and
            # populated in another (e.g. an optional fee not yet charged).
            "amount": {"type": ["null", "number"], "multipleOf": 0.01},
        },
        "type": "object",
    },
}

# batch 1: "amount" is all-null. batch 2: "amount" has real values. Same
# stream, same schema -- this is the exact drift trigger.
RECORDS = [
    {"id": 1, "name": "a", "amount": None},
    {"id": 2, "name": "b", "amount": None},
    {"id": 3, "name": "c", "amount": 12.34},
    {"id": 4, "name": "d", "amount": -56.78},
]


def _singer_stream(records: list[dict]) -> str:
    lines = [json.dumps(STREAM_SCHEMA)]
    lines += [
        json.dumps({"type": "RECORD", "stream": "invoices", "record": r})
        for r in records
    ]
    return "\n".join(lines) + "\n"


def _make_config(tmp_path: Path, **overrides) -> dict:
    config = {
        "format": {"format_type": "parquet"},
        "compression": "gzip",
        "cloud_provider": {
            "cloud_provider_type": "aws",
            "aws": {"aws_bucket": str(tmp_path / "bucket"), "aws_region": "us-east-1"},
        },
        "prefix": "p",
        "append_date_to_prefix": False,
        "append_date_to_filename": False,
        "use_raw_stream_name": False,
        "max_batch_size": 2,
    }
    config.update(overrides)
    return config


@pytest.fixture
def local_parquet_fs(monkeypatch):
    """Swaps FormatParquet's S3FileSystem for a LocalFileSystem so these
    tests can write and read back real Parquet files without MinIO/moto
    (moto only mocks botocore; pyarrow's S3FileSystem bypasses it via
    arrow's own C++ AWS client -- confirmed by running the unmodified code
    against a moto-mocked bucket: it still tries to authenticate for real)."""
    monkeypatch.setattr(
        format_parquet_mod.FormatParquet,
        "create_filesystem",
        lambda self, *a, **kw: pyarrow.fs.LocalFileSystem(),
    )


def _run(tmp_path: Path, records: list[dict], **config_overrides) -> None:
    logging.getLogger("target-s3").setLevel(logging.ERROR)
    # pyarrow's LocalFileSystem, unlike real S3, doesn't auto-create
    # "directories" for a key's path segments. append_date_to_prefix is off
    # in _make_config, so the directory part of the key is fully static:
    # bucket/prefix/stream_name/ -- only the filename itself varies
    # (part-counter + uuid).
    (tmp_path / "bucket" / "p" / "invoices").mkdir(parents=True, exist_ok=True)
    target = Targets3(config=_make_config(tmp_path, **config_overrides))
    target.listen(io.StringIO(_singer_stream(records)))


def _written_parquet_files(tmp_path: Path) -> list[Path]:
    return sorted((tmp_path / "bucket").rglob("*.parquet"))


class TestSchemaStabilityAcrossBatches:
    def test_default_path_produces_identical_schema_across_batches(
        self, tmp_path, local_parquet_fs
    ):
        _run(tmp_path, RECORDS)  # get_schema_from_tap defaults to true

        files = _written_parquet_files(tmp_path)
        assert len(files) == 2, f"expected 2 batch files, got {files}"

        tables = [pyarrow.parquet.read_table(f) for f in files]
        assert tables[0].schema.equals(tables[1].schema), (
            f"schemas differ across batches:\n{tables[0].schema}\nvs\n{tables[1].schema}"
        )

        # The proof that matters: a single reader can load both files
        # together, exactly what ClickHouse's s3(..., 'Parquet', ...) glob
        # over multiple files needs to do.
        combined = pyarrow.concat_tables(tables)
        assert combined.num_rows == 4
        assert sorted(combined.column("id").to_pylist()) == [1, 2, 3, 4]

        # The all-null batch's "amount" column must be typed as a real
        # number (from the declared schema), not pyarrow's null() type --
        # otherwise this would "pass" schema-equality trivially by both
        # being null, without actually proving the fix.
        assert tables[0].schema.field("amount").type == pyarrow.float64()
        assert tables[1].schema.field("amount").type == pyarrow.float64()

    def test_escape_hatch_reproduces_the_original_drift(self, tmp_path, local_parquet_fs):
        # Documents the failure mode this fork fixed: with the per-batch
        # inference path explicitly opted into, the same two batches DO
        # produce incompatible schemas. Not a regression -- get_schema_from_tap
        # must be explicitly set to false to reach this path.
        #
        # Note batch 2's "amount" infers as decimal128, not float64: this
        # path parses records with parse_float=Decimal (fixed alongside the
        # schema-stability default -- see Targets3.deserialize_json), so
        # pyarrow infers a decimal type from the real Decimal values. Still
        # a real schema mismatch against batch 1's null-typed column, just a
        # richer one than a naive float64 assumption would suggest.
        _run(tmp_path, RECORDS, format={
            "format_type": "parquet",
            "format_parquet": {"get_schema_from_tap": False},
        })

        files = _written_parquet_files(tmp_path)
        assert len(files) == 2

        tables = [pyarrow.parquet.read_table(f) for f in files]
        assert tables[0].schema.field("amount").type == pyarrow.null()
        assert pyarrow.types.is_decimal(tables[1].schema.field("amount").type)
        assert not tables[0].schema.equals(tables[1].schema)

        with pytest.raises(pyarrow.lib.ArrowInvalid):
            pyarrow.concat_tables(tables)

    def test_empty_format_parquet_dict_still_uses_stable_default(
        self, tmp_path, local_parquet_fs
    ):
        # format_parquet: {} (present but empty, e.g. sample-config.json's
        # own default) is falsy in Python -- a naive `if format_parquet and
        # format_parquet.get(...)` check silently falls through to the
        # unstable per-batch path even though get_schema_from_tap was never
        # actually set to false. Guards against that regressing.
        _run(tmp_path, RECORDS, format={"format_type": "parquet", "format_parquet": {}})

        files = _written_parquet_files(tmp_path)
        tables = [pyarrow.parquet.read_table(f) for f in files]
        assert tables[0].schema.equals(tables[1].schema)
        assert tables[0].schema.field("amount").type == pyarrow.float64()


class TestParquetCompression:
    @pytest.mark.parametrize(
        ("compression", "expected_codec"),
        [("gzip", "GZIP"), ("none", "UNCOMPRESSED")],
    )
    def test_compression_setting_maps_to_a_real_parquet_codec(
        self, tmp_path, local_parquet_fs, compression, expected_codec
    ):
        _run(tmp_path, RECORDS[:2], compression=compression)

        [f] = _written_parquet_files(tmp_path)
        metadata = pyarrow.parquet.ParquetFile(f).metadata
        codec = metadata.row_group(0).column(0).compression
        assert codec == expected_codec, f"expected {expected_codec}, got {codec}"

        # Still a valid, readable Parquet file either way.
        table = pyarrow.parquet.read_table(f)
        assert table.num_rows == 2

    def test_filename_has_no_external_gzip_suffix(self, tmp_path, local_parquet_fs):
        # Parquet's compression is internal to the file (per-column-chunk
        # codec) -- a "*.parquet.gz" filename would wrongly imply an
        # external gzip wrapper, which is what JSON/JSONL/CSV use and
        # Parquet does not.
        _run(tmp_path, RECORDS[:2], compression="gzip")

        [f] = _written_parquet_files(tmp_path)
        assert f.name.endswith(".parquet")
        assert ".gz" not in f.name
