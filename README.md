# target-s3

[![PyPI version](https://img.shields.io/pypi/v/plinxore-target-s3.svg)](https://pypi.org/project/plinxore-target-s3/)
[![CI](https://github.com/plinxore/target-s3/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/plinxore/target-s3/actions/workflows/ci.yml)
[![Python](https://img.shields.io/pypi/pyversions/plinxore-target-s3.svg)](https://pypi.org/project/plinxore-target-s3/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

`target-s3` is a multi-format/multi-cloud Singer target, writing streams to S3-compatible object storage (AWS S3, MinIO, ...) as JSON, JSONL, CSV, or Parquet.

Built with the [Meltano Target SDK](https://sdk.meltano.com).

> **Provenance:** this is the `plinxore` fork of [`crowemi/target-s3`](https://github.com/crowemi/target-s3), matured for production use: real gzip compression (the original wrote plaintext behind a misleading `.gz` extension), corrected JSONL datetime serialization, configurable handling of unparseable source dates (e.g. legacy MySQL/MyISAM zero-dates), a collision-safe batch filename scheme that guarantees a rerun can never silently overwrite a previous run's files, and a Parquet writer with a schema kept stable across batches (derived once from the Singer SCHEMA rather than re-inferred per batch) plus exact decimal precision for DECIMAL-shaped columns (`decimal128`, not `float64`) -- see [Parquet output](#parquet-output) below. See [NOTICE](NOTICE) for the full derivation statement required by the Apache 2.0 license.

## Installation

```bash
pip install plinxore-target-s3
```

## Configuration

### Accepted Config Options

```json
{
    "format": {
        "format_type": "json",
        "format_parquet": {
            "get_schema_from_tap": true|false,
            "validate": true|false
        },
        "format_json": {},
        "format_csv": {}
    },
    "cloud_provider": {
        "cloud_provider_type": "aws",
        "aws": {
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",
            "aws_region": "us-west-2",
            "aws_profile_name": "test-profile",
            "aws_bucket": "test-bucket",
            "aws_endpoint_override": "http://localhost:4566"
        }
    },
    "compression": "gzip|none",
    "datetime_error_treatment": "null|max|error",
    "prefix": "path/to/output",
    "stream_name_path_override": "StreamName",
    "include_process_date": true|false,
    "append_date_to_prefix": true|false,
    "partition_name_enabled": true|false,
    "use_raw_stream_name": true|false,
    "append_date_to_prefix_grain": "day",
    "append_date_to_filename": true|false,
    "append_date_to_filename_grain": "microsecond",
    "append_uuid": true|false,
    "flattening_enabled": true|false,
    "flattening_max_depth": int,
    "max_batch_age": int,
    "max_batch_size": int,
    "partition_by": ["tenant=${TENANT}", "dt=${CURRENT_DATE_MINUTE_LEVEL}"]
}
```
`format.format_parquet.get_schema_from_tap` [`Boolean`, default: `True`] - derives the Parquet schema once from the stream's Singer SCHEMA message and reuses it for every batch of that stream, instead of letting pyarrow infer a schema independently per batch. See [Parquet output](#parquet-output) for why this is the default. Set `False` to fall back to per-batch inference (doesn't work with `anyOf` types or complex data not defined at element level; not compatible with `validate`).

`format.format_parquet.validate` [`Boolean`, default: `False`] - this flag determines whether the data types of incoming data elements should be validated. When set `True`, a schema is created from the first record and all subsequent records that don't match that data type are cast. Only applies to the per-batch-inference path (`get_schema_from_tap: false`).

- `partition_by` [`Array[String]`, optional]: List of key-value strings (e.g., 'tenant=${TENANT}') to be inserted as partition folders **after the stream name** in the S3 key path. For example, if `partition_by: ['tenant=${TENANT}', 'dt=${CURRENT_DATE_MINUTE_LEVEL}']` and the stream is `Account`, the S3 key will look like:

  ```
  bucket/prefix/Account/tenant=${TENANT}/dt=${CURRENT_DATE_MINUTE_LEVEL}/...
  ```

- `compression` [`String`, default: `"gzip"`, allowed: `"none"`, `"gzip"`] - compression applied to written files. `"gzip"` produces real gzip output (verified with `gzip -t`, not just a `.gz`-named file); `"none"` disables compression.

- `datetime_error_treatment` [`String`, default: `"null"`, allowed: `"null"`, `"max"`, `"error"`] - how to handle date/date-time values the SDK can't parse, such as legacy MySQL/MyISAM zero-dates (`"0000-00-00 00:00:00"`). `"null"` replaces the value with null so the run continues; `"error"` aborts the run (the SDK's own default behavior if this were unset).

## Parquet output

By default (`format.format_parquet.get_schema_from_tap: true`), the Parquet schema for a stream is derived once from its Singer SCHEMA message and reused for every batch, instead of letting pyarrow infer a schema independently per batch. Per-batch inference is what causes schema drift: a batch where a column happens to be all-null infers that column as pyarrow's `null` type, while a later batch with real values infers a real type -- producing files for the same stream that can't be read together by a single reader (e.g. a multi-file glob in ClickHouse's `s3(..., 'Parquet', ...)` or DuckDB's `read_parquet(...)`).

DECIMAL-shaped numeric columns (a Singer `number` type with `multipleOf` set, e.g. a MySQL `DECIMAL` column) are mapped to `decimal128`, not `float64`, with the scale taken from `multipleOf` and a fixed precision of 38. `float64` cannot represent every decimal value exactly; `decimal128` carries values like `1234.56` through bit-for-bit, matching the exact-precision guarantee this target already provides for JSONL/JSON via `decimal.Decimal`.

Parquet compression uses its own internal per-column-chunk codec (mapped from the `compression` setting), not the external gzip wrapper JSON/JSONL/CSV use -- so a compressed Parquet file keeps a plain `.parquet` extension rather than `.parquet.gz`.

## Object key uniqueness & idempotency

Every object key ends in `-part-{batch_number:05d}` (e.g.
`.../year=2026/month=08/day=11/invoices-part-00001.jsonl.gz`), appended after
any `append_date_to_filename` suffix, plus a UUID unless `append_uuid: false`
(e.g. `...invoices-part-00001-3f2504e0-4f89-11d3-9a0c-0305e82c3301.jsonl.gz`):

- `batch_number` is a monotonic counter, reset to 1 at the start of every run, giving batches a readable, ordered position *within that run*. It is always present, in both modes -- it's what keeps batches of the *same* run from colliding with each other.
- `append_uuid` [`Boolean`, default: `true`] - whether a UUID, generated fresh per batch, is also appended. This is what decides whether object keys are reusable across runs:
  - **`true` (default)**: two different runs of the same stream (e.g. a same-day retry) both start counting batches at 1, so the counter alone cannot prevent one run's files from colliding with another's -- the UUID makes that collision structurally impossible regardless of how many times a stream is replayed. **This target only guarantees that a rerun can never silently overwrite a previous run's objects** -- it does not decide whether a rerun's data should *replace* or *add to* what's already been written. An **incremental** table reads via a glob over the date partition (`year=*/month=*/day=*/*.jsonl.gz`) and is expected to accumulate every run's files this way.
  - **`false`**: object keys become reusable across runs with the same batch layout (same number of batches, same order), so a rerun overwrites the previous run's objects on matching keys instead of accumulating alongside them.

**`append_uuid: false` alone is not a full-refresh/overwrite feature -- it is only the naming half of one.** A real idempotent overwrite needs all three of:

1. `append_uuid: false` (this target) -- so a rerun's keys match the previous run's.
2. Purging the key prefix before the run (**orchestration**'s job) -- so a rerun with *fewer* batches than the previous one doesn't leave orphaned objects behind. If run 1 writes `part-00001` through `part-00003` and run 2 (smaller) only writes `part-00001`, `part-00002` and `part-00003` from run 1 survive untouched unless something purges the prefix first -- this target has no prefix-purge or deduplication logic and never will; it only controls naming.
3. Reading from a fixed path (the **consumer**'s job, e.g. which path/glob a downstream reader like ClickHouse's `s3()` table function is pointed at) -- an incremental reader globbing over every run's files would just see the overwritten data as more files, not a replacement.

This target only ever does (1). Deciding whether a given table needs (1)+(2)+(3) (full-refresh/overwrite) or none of them (incremental/accumulate, the default) is a pipeline decision made outside this target.

## Capabilities

* `about`
* `stream-maps`
* `schema-flattening`

## Settings

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

## Usage

You can easily run `target-s3` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Direct CLI (without Meltano)

```bash
target-s3 --version
target-s3 --about
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-s3 --config /path/to/target-s3-config.json
```

### Via Meltano (recommended)

```bash
# Install the Meltano CLI (if not already done)
pipx install meltano

# Add this variant explicitly
meltano add loader target-s3 --variant plinxore

# Install the plugins declared in meltano.yml
meltano install

# Run the pipeline
meltano run tap-mysql target-s3
```

## Development

### Tests

```bash
uv run pytest              # fast suite (excludes the load test)
uv run pytest -m load      # permanent >=1M-record memory/throughput regression test (several minutes)
```

### SDK Dev Guide

See the [Meltano Singer SDK dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to
develop your own Singer taps and targets.
