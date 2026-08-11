"""s3 target sink class, which handles writing streams."""

from __future__ import annotations
import logging

from singer_sdk.helpers._typing import DatetimeErrorTreatmentEnum
from singer_sdk.sinks import BatchSink

from target_s3.formats.format_base import FormatBase, format_type_factory
from target_s3.formats.format_parquet import FormatParquet
from target_s3.formats.format_csv import FormatCsv
from target_s3.formats.format_json import FormatJson
from target_s3.formats.format_jsonl import FormatJsonl


LOGGER = logging.getLogger("target-s3")
FORMAT_TYPE = {"parquet": FormatParquet, "csv": FormatCsv, "json": FormatJson, "jsonl": FormatJsonl}
DATETIME_ERROR_TREATMENT = {
    "error": DatetimeErrorTreatmentEnum.ERROR,
    "max": DatetimeErrorTreatmentEnum.MAX,
    "null": DatetimeErrorTreatmentEnum.NULL,
}


class s3Sink(BatchSink):
    """s3 target sink class."""

    def __init__(
        self,
        target: any,
        stream_name: str,
        schema: dict,
        key_properties: list[str] | None,
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        # what type of file are we building?
        self.format_type = self.config.get("format", None).get("format_type", None)
        self.schema = schema
        if self.format_type:
            if self.format_type not in FORMAT_TYPE:
                raise Exception(
                    f"Unknown file type specified. {key_properties['type']}"
                )
        else:
            raise Exception("No file type supplied.")
        # Monotonic, per-stream, per-run counter used to guarantee a unique
        # object key per batch (see create_key()'s "-part-NNNNN" suffix).
        # Deliberately clock-independent: a wall-clock timestamp can collide
        # under load and breaks idempotency (a rerun of the same extraction
        # would mint new filenames instead of overwriting the same ones).
        self._batch_counter = 0

    @property
    def max_size(self) -> int:
        """Get maximum batch size.

        Returns:
            Maximum batch size
        """
        return self.config.get("max_batch_size", 10000)

    @property
    def datetime_error_treatment(self) -> DatetimeErrorTreatmentEnum:
        """Get the treatment for datetime-like values the SDK can't parse.

        Source systems such as legacy MySQL/MyISAM commonly contain
        unparseable date-time strings (e.g. zero-dates like
        "0000-00-00 00:00:00"). By default the SDK raises and aborts the
        whole run; this makes that behavior config-driven instead.

        Returns:
            The configured datetime error treatment (default: null out the
            value rather than crash).
        """
        return DATETIME_ERROR_TREATMENT[self.config.get("datetime_error_treatment", "null")]

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
        # add stream name to context
        context["stream_name"] = self.stream_name
        context["logger"] = self.logger
        context["stream_schema"] = self.schema
        self._batch_counter += 1
        context["batch_number"] = self._batch_counter
        # creates new object for each batch
        format_type_client = format_type_factory(
            FORMAT_TYPE[self.format_type], self.config, context
        )
        # force base object_type_client to object_type_base class
        assert (
            isinstance(format_type_client, FormatBase) is True
        ), f"format_type_client must be of type Base; Type: {type(self.format_type_client)}."

        format_type_client.run()
