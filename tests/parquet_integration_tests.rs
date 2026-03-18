#![cfg(feature = "parquet")]

mod common;
use arrow::array::{Array, Float64Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use common::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn parquet_batches(args: &[&str], envs: &[(&str, &str)]) -> Vec<RecordBatch> {
    let mut cmd = sunce_command();
    for (key, value) in envs {
        cmd.env(key, value);
    }
    let output = cmd
        .args(args)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(output))
        .expect("Failed to create Parquet reader")
        .build()
        .expect("Failed to build Parquet reader");
    reader
        .collect::<Result<Vec<_>, _>>()
        .expect("Failed to read batches")
}

fn parquet_single_batch(args: &[&str], envs: &[(&str, &str)]) -> RecordBatch {
    let batches = parquet_batches(args, envs);
    assert_eq!(batches.len(), 1, "Should have exactly one batch");
    batches.into_iter().next().unwrap()
}

fn schema_field_names(batch: &RecordBatch) -> Vec<String> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect()
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn string_array<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing {name} column"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("{name} should be StringArray"))
}

fn float_array<'a>(batch: &'a RecordBatch, name: &str) -> &'a Float64Array {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing {name} column"))
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap_or_else(|| panic!("{name} should be Float64Array"))
}

#[test]
fn test_parquet_position_basic() {
    let batch = parquet_single_batch(
        &[
            "--format=PARQUET",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ],
        &[],
    );
    assert_eq!(
        schema_field_names(&batch),
        vec!["dateTime", "azimuth", "zenith"]
    );
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_parquet_position_with_inputs() {
    let batch = parquet_single_batch(
        &[
            "--format=PARQUET",
            "--show-inputs",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ],
        &[],
    );
    assert_eq!(
        schema_field_names(&batch),
        vec![
            "latitude",
            "longitude",
            "elevation",
            "pressure",
            "temperature",
            "dateTime",
            "deltaT",
            "azimuth",
            "zenith",
        ]
    );
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_parquet_sunrise_basic() {
    let batch = parquet_single_batch(
        &["--format=PARQUET", "52.0", "13.4", "2024-01-01", "sunrise"],
        &[],
    );
    let schema = batch.schema();
    assert_eq!(
        schema_field_names(&batch),
        vec!["dateTime", "type", "sunrise", "transit", "sunset"]
    );
    assert!(schema.field_with_name("sunrise").unwrap().is_nullable());
    assert!(schema.field_with_name("sunset").unwrap().is_nullable());
    assert!(!schema.field_with_name("transit").unwrap().is_nullable());
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_parquet_sunrise_with_twilight() {
    let batch = parquet_single_batch(
        &[
            "--format=PARQUET",
            "--show-inputs",
            "52.0",
            "13.4",
            "2024-06-21",
            "sunrise",
            "--twilight",
        ],
        &[],
    );
    let schema = batch.schema();
    assert_eq!(
        schema_field_names(&batch),
        vec![
            "latitude",
            "longitude",
            "dateTime",
            "deltaT",
            "type",
            "sunrise",
            "transit",
            "sunset",
            "civil_start",
            "civil_end",
            "nautical_start",
            "nautical_end",
            "astronomical_start",
            "astronomical_end",
        ]
    );
    for field in [
        "civil_start",
        "civil_end",
        "nautical_start",
        "nautical_end",
        "astronomical_start",
        "astronomical_end",
    ] {
        assert!(
            schema.field_with_name(field).unwrap().is_nullable(),
            "{field} should be nullable"
        );
    }
}

#[test]
fn test_parquet_multiple_coordinates() {
    let batches = parquet_batches(
        &[
            "--format=PARQUET",
            "52:53:0.5",
            "13:14:0.5",
            "2024-01-01T12:00:00",
            "position",
        ],
        &[],
    );
    assert!(!batches.is_empty());
    assert_eq!(total_rows(&batches), 9);
}

#[test]
fn test_parquet_streaming_behavior() {
    let batches = parquet_batches(
        &[
            "--format=PARQUET",
            "50:52:0.1",
            "10:12:0.1",
            "2024-01-01T12:00:00",
            "position",
        ],
        &[],
    );
    let rows = total_rows(&batches);
    assert!(
        (400..=450).contains(&rows),
        "Expected about 441 rows, got {rows}"
    );
    assert_eq!(batches.len(), 1, "Should fit in one batch");
}

#[test]
fn test_parquet_consistency_with_csv() {
    let csv_text = String::from_utf8(
        SunceTest::new()
            .args([
                "--format=CSV",
                "52.0",
                "13.4",
                "2024-01-01T12:00:00",
                "position",
            ])
            .get_output()
            .stdout,
    )
    .unwrap();
    let (csv_headers, csv_rows) = parse_csv_output(&csv_text);
    assert_eq!(csv_headers, vec!["dateTime", "azimuth", "zenith"]);
    assert_eq!(csv_rows.len(), 1);
    let csv_record = csv_row_map(&csv_headers, &csv_rows[0]);

    let batch = parquet_single_batch(
        &[
            "--format=PARQUET",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ],
        &[],
    );
    assert_eq!(batch.num_columns(), 3);
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        string_array(&batch, "dateTime").value(0),
        csv_record["dateTime"]
    );

    let csv_azimuth = csv_record["azimuth"].parse::<f64>().unwrap();
    let csv_zenith = csv_record["zenith"].parse::<f64>().unwrap();
    assert!((float_array(&batch, "azimuth").value(0) - csv_azimuth).abs() <= 1e-4);
    assert!((float_array(&batch, "zenith").value(0) - csv_zenith).abs() <= 1e-4);
}

#[test]
fn test_parquet_timezone_preservation() {
    let batch = parquet_single_batch(
        &[
            "--format=PARQUET",
            "52.0",
            "13.4",
            "2024-06-21T12:00:00",
            "position",
        ],
        &[("TZ", "Europe/Berlin")],
    );
    let datetime_col = batch.column_by_name("dateTime").unwrap();
    assert!(matches!(datetime_col.data_type(), DataType::Utf8));
    assert_eq!(
        string_array(&batch, "dateTime").value(0),
        "2024-06-21T12:00:00+02:00"
    );
}

#[test]
fn test_parquet_sunrise_type_consistency() {
    let batch = parquet_single_batch(
        &["--format=PARQUET", "52.0", "13.4", "2024-06-21", "sunrise"],
        &[],
    );
    assert_eq!(string_array(&batch, "type").value(0), "NORMAL");
}

#[test]
fn test_parquet_sunrise_null_handling() {
    let batch = parquet_single_batch(
        &["--format=PARQUET", "90.0", "0.0", "2024-06-21", "sunrise"],
        &[],
    );
    assert_eq!(string_array(&batch, "type").value(0), "ALL_DAY");
    assert!(string_array(&batch, "sunrise").is_null(0));
    assert!(string_array(&batch, "sunset").is_null(0));
    assert!(!string_array(&batch, "transit").is_null(0));
}

#[test]
fn test_parquet_sunrise_twilight_null_handling() {
    let batch = parquet_single_batch(
        &[
            "--format=PARQUET",
            "90.0",
            "0.0",
            "2024-06-21",
            "sunrise",
            "--twilight",
        ],
        &[],
    );
    for field in [
        "civil_start",
        "civil_end",
        "nautical_start",
        "nautical_end",
        "astronomical_start",
        "astronomical_end",
    ] {
        assert!(
            string_array(&batch, field).is_null(0),
            "{field} should be null"
        );
    }
}
