"""s3 target class."""

from __future__ import annotations

from singer_sdk.target_base import Target
from singer_sdk import typing as th

from target_s3.formats.format_base import DATE_GRAIN

from target_s3.sinks import (
    s3Sink,
)


class Targets3(Target):
    """Singer target for S3-compatible object storage (plinxore fork)."""

    name = "target-s3"
    # Name of the actually installed PyPI package (different from `name`
    # above), required for get_plugin_version() to resolve the right version.
    package_name = "plinxore-target-s3"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "format",
            th.ObjectType(
                th.Property(
                    "format_type",
                    th.StringType,
                    required=True,
                    allowed_values=[
                        "parquet",
                        "json",
                        "jsonl",
                    ],  # TODO: configure this from class
                ),
                th.Property(
                    "format_parquet",
                    th.ObjectType(
                        th.Property(
                            "validate",
                            th.BooleanType,
                            required=False,
                            default=False,
                        ),
                        th.Property(
                            "get_schema_from_tap",
                            th.BooleanType,
                            required=False,
                            default=True,
                            description="Derive the Parquet schema once from the "
                            "stream's Singer SCHEMA message and reuse it for every "
                            "batch of that stream, instead of letting pyarrow infer "
                            "a schema independently per batch. Per-batch inference "
                            "(set this to false to use it) is what causes schema "
                            "drift between Parquet files of the same stream -- a "
                            "batch where a column happens to be all-null infers "
                            "that column as pyarrow's null type, while a later "
                            "batch with real values infers a real type, producing "
                            "files that can't be read together. Doesn't work with "
                            "'anyOf' types or when complex data is not defined at "
                            "element level. Doesn't work with the validate option "
                            "(validate only applies to the per-batch-inference path)."
                        ),
                    ),
                    required=False,
                ),
                th.Property(
                    "format_json",
                    th.ObjectType(),
                    required=False,
                ),
                th.Property(
                    "format_csv",
                    th.ObjectType(),
                    required=False,
                ),
            ),
        ),
        th.Property(
            "compression",
            th.StringType,
            description="Compression to apply to written files. 'gzip' appends a "
            ".gz suffix and produces real gzip output; 'none' disables compression.",
            required=False,
            allowed_values=["none", "gzip"],
            default="gzip",
        ),
        th.Property(
            "datetime_error_treatment",
            th.StringType,
            description="How to handle date/date-time values the SDK cannot parse "
            "(e.g. MySQL/MyISAM zero-dates like '0000-00-00 00:00:00'). 'null' "
            "replaces the value with null, 'max' replaces it with the max "
            "representable timestamp, 'error' aborts the run.",
            required=False,
            allowed_values=["null", "max", "error"],
            default="null",
        ),
        th.Property(
            "cloud_provider",
            th.ObjectType(
                th.Property(
                    "cloud_provider_type",
                    th.StringType,
                    required=True,
                    allowed_values=["aws"],  # TODO: configure this from class
                ),
                th.Property(
                    "aws",
                    th.ObjectType(
                        th.Property(
                            "aws_access_key_id",
                            th.StringType,
                            required=False,
                            secret=True,
                        ),
                        th.Property(
                            "aws_secret_access_key",
                            th.StringType,
                            required=False,
                            secret=True,
                        ),
                        th.Property(
                            "aws_session_token",
                            th.StringType,
                            required=False,
                            secret=True,
                        ),
                        th.Property(
                            "aws_region",
                            th.StringType,
                            required=True,
                        ),
                        th.Property(
                            "aws_profile_name",
                            th.StringType,
                            required=False,
                        ),
                        th.Property(
                            "aws_bucket",
                            th.StringType,
                            required=True,
                        ),
                        th.Property(
                            "aws_endpoint_override",
                            th.StringType,
                            required=False,
                        ),
                    ),
                    required=False,
                ),
            ),
        ),
        th.Property(
            "prefix",
            th.StringType,
            description="The prefix for the key.",
        ),
        th.Property(
            "stream_name_path_override",
            th.StringType,
            description="The S3 key stream name override.",
        ),
        th.Property(
            "include_process_date",
            th.BooleanType,
            description="A flag indicating whether to append _process_date to record.",
            default=False,
        ),
        th.Property(
            "use_raw_stream_name",
            th.BooleanType,
            description="A flag to force the filename to be identical to the stream name.",
            default=False,
        ),
        th.Property(
            "append_date_to_prefix",
            th.BooleanType,
            description="A flag to append the date to the key prefix.",
            default=True,
        ),
        th.Property(
            "partition_name_enabled",
            th.BooleanType,
            description="A flag (only works if append_date_to_prefix is enabled) to have partitioning name formatted e.g. 'year=2023/month=01/day=01'.",
            default=False,
        ),
        th.Property(
            "append_date_to_prefix_grain",
            th.StringType,
            description="The grain of the date to append to the prefix.",
            allowed_values=list(DATE_GRAIN.keys()),
            default="day",
        ),
        th.Property(
            "append_date_to_filename",
            th.BooleanType,
            description="A flag to append the date to the key filename, for "
            "readability. Every filename already carries a monotonic "
            "'-part-NNNNN' batch counter regardless of this setting, so "
            "this is purely cosmetic -- it is not what guarantees "
            "batch-to-batch uniqueness.",
            default=True,
        ),
        th.Property(
            "append_date_to_filename_grain",
            th.StringType,
            description="The grain of the date to append to the filename.",
            allowed_values=list(DATE_GRAIN.keys()),
            default="day",
        ),
        th.Property(
            "max_batch_age",
            th.NumberType,
            description="Maximum time in minutes between state messages when records are streamed in.",
            required=False,
            default=5.0,
        ),
        th.Property(
            "max_batch_size",
            th.IntegerType,
            description="Maximum size of batches when records are streamed in.",
            required=False,
            default=10000,
        ),
        th.Property(
            "partition_by",
            th.ArrayType(th.StringType),
            required=False,
            description="List of key-value strings (e.g., 'tenant=${TENANT}') to prepend as partitions in the S3 key path after the stream name.",
        ),
    ).to_dict()

    default_sink_class = s3Sink

    @property
    def _MAX_RECORD_AGE_IN_MINUTES(self) -> float:  # type: ignore
        return float(self.config.get("max_batch_age", 5.0))

    # No deserialize_json override: singer_sdk's own default (parse_float=
    # decimal.Decimal) is correct for every path now. It used to be
    # overridden to parse plain floats when using Parquet's
    # get_schema_from_tap, because that path's schema hardcoded every
    # number field to float64 and pyarrow's Table.from_pydict() rejects
    # Decimal values against an explicit float64 field. Now that
    # create_schema() maps DECIMAL-shaped fields (multipleOf present) to
    # pyarrow.decimal128 instead, Decimal values are exactly what's needed
    # -- create_dataframe() downcasts to float only for the fields that are
    # still float64 (see the decimal_fields handling there).


if __name__ == "__main__":
    Targets3.cli()
