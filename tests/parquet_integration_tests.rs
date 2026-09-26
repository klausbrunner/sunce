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
fn test_parquet_consistency_with_csv() {
    let csv_text = String::from_utf8(
        sunce_command()
            .args([
                "--format=CSV",
                "52.0",
                "13.4",
                "2024-01-01T12:00:00",
                "position",
            ])
            .output()
            .unwrap()
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
    assert_eq!(schema_field_names(&batch), csv_headers);
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
fn preserves_offsets_for_equal_instants() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("instants.csv");
    write_text_file(
        &path,
        "52 13.4 2024-01-01T12:00:00+00:00\n52 13.4 2024-01-01T13:00:00+01:00\n",
    );
    let input = format!("@{}", path.display());
    let command = "position";
    let batch = parquet_single_batch(&[&input, command, "--format=parquet"], &[]);
    let dates = string_array(&batch, "dateTime");
    assert_eq!(dates.value(0), "2024-01-01T12:00:00+00:00");
    assert_eq!(dates.value(1), "2024-01-01T13:00:00+01:00");
}

#[test]
fn event_schema_and_values_match_csv_including_empty_dates() {
    for (lat, lon, date, twilight) in [
        ("52", "13.4", "2024-06-21", false),
        ("52", "13.4", "2024-06-21", true),
        ("78.216667", "15.633333", "2020-04-16", false),
        ("90", "179.9", "2020-06-10", false),
    ] {
        let mut args = vec![
            "--timezone=UTC",
            "--show-inputs",
            "--deltat=69.184",
            lat,
            lon,
            date,
            "events",
        ];
        if twilight {
            args.push("--twilight");
        }
        let output = sunce_command()
            .args(&args)
            .arg("--format=csv")
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        let (headers, rows) = parse_csv_output(std::str::from_utf8(&output).unwrap());
        args.push("--format=parquet");
        let batch = parquet_single_batch(&args, &[]);
        assert_eq!(schema_field_names(&batch), headers);
        assert_eq!(batch.num_rows(), rows.len());
        for (i, row) in rows.iter().enumerate() {
            for (name, value) in headers.iter().zip(row) {
                if ["latitude", "longitude", "deltaT"].contains(&name.as_str()) {
                    assert!(
                        (float_array(&batch, name).value(i) - value.parse::<f64>().unwrap()).abs()
                            <= 0.0005
                    );
                } else {
                    let column = string_array(&batch, name);
                    assert_eq!(column.is_null(i), value.is_empty());
                    if !value.is_empty() {
                        assert_eq!(column.value(i), value);
                    }
                }
            }
        }
        let schema = batch.schema();
        for name in ["event", "time"] {
            assert!(schema.field_with_name(name).unwrap().is_nullable());
        }
        assert!(!schema.field_with_name("date").unwrap().is_nullable());
    }
}
