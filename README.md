# target-s3

`target-s3` is inteded to be a multi-format/multi-cloud Singer target.

Build with the [Meltano Target SDK](https://sdk.meltano.com).


## Configuration

### Accepted Config Options

```json
{
    "format": {
        "format_type": "json",
        "format_parquet": {
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
    "prefix": "path/to/output",
    "stream_name_path_override": "StreamName",
    "include_process_date": true|false,
    "append_date_to_prefix": true|false,
    "partition_name_enabled": true|false,
    "use_raw_stream_name": true|false,
    "append_date_to_prefix_grain": "day",
    "append_date_to_filename": true|false,
    "append_date_to_filename_grain": "microsecond",
    "flattening_enabled": true|false,
    "flattening_max_depth": int,
    "max_batch_age": int,
    "max_batch_size": int,
    "partition_by": ["tenant=${TENANT}", "dt=${CURRENT_DATE_MINUTE_LEVEL}"]
}
```
`format.format_parquet.validate` [`Boolean`, default: `False`] - this flag determines whether the data types of incoming data elements should be validated. When set `True`, a schema is created from the first record and all subsequent records that don't match that data type are cast.

- `partition_by` [`Array[String]`, optional]: List of key-value strings (e.g., 'tenant=${TENANT}') to be inserted as partition folders **after the stream name** in the S3 key path. For example, if `partition_by: ['tenant=${TENANT}', 'dt=${CURRENT_DATE_MINUTE_LEVEL}']` and the stream is `Account`, the S3 key will look like:

  ```
  bucket/prefix/Account/tenant=${TENANT}/dt=${CURRENT_DATE_MINUTE_LEVEL}/...
  ```

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

### Executing the Target Directly

```bash
target-s3 --version
target-s3 --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-s3 --config /path/to/target-s3-config.json
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano Singer SDK to
develop your own Singer taps and targets.
