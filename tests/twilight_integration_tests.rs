mod common;
use common::*;
use serde_json::Value;
use std::collections::HashMap;

fn output_text(args: &[&str]) -> String {
    String::from_utf8(SunceTest::new().args(args).get_output().stdout).unwrap()
}

fn twilight_csv_record(extra_args: &[&str]) -> HashMap<String, String> {
    let mut args = vec!["--format=csv"];
    args.extend_from_slice(extra_args);
    args.extend_from_slice(&[
        "--timezone=UTC",
        "52.0",
        "13.4",
        "2024-06-21",
        "sunrise",
        "--twilight",
    ]);
    parse_csv_single_record_map(&output_text(&args))
}

fn assert_core_twilight_times(record: &HashMap<String, String>) {
    assert_time_close(
        record.get("sunrise").expect("missing sunrise"),
        "2024-06-21T02:46:15+00:00",
        0,
    );
    assert_time_close(
        record.get("transit").expect("missing transit"),
        "2024-06-21T11:08:18+00:00",
        0,
    );
    assert_time_close(
        record.get("sunset").expect("missing sunset"),
        "2024-06-21T19:30:20+00:00",
        0,
    );
    assert_time_close(
        record.get("civil_start").expect("missing civil_start"),
        "2024-06-21T01:57:19+00:00",
        0,
    );
    assert_time_close(
        record.get("civil_end").expect("missing civil_end"),
        "2024-06-21T20:19:16+00:00",
        0,
    );
    assert_time_close(
        record
            .get("nautical_start")
            .expect("missing nautical_start"),
        "2024-06-21T00:38:45+00:00",
        0,
    );
    assert_time_close(
        record.get("nautical_end").expect("missing nautical_end"),
        "2024-06-21T21:37:47+00:00",
        1,
    );
}

fn assert_core_twilight_json_times(json: &Value) {
    assert_time_close(
        json.get("sunrise")
            .and_then(Value::as_str)
            .expect("missing sunrise"),
        "2024-06-21T02:46:15+00:00",
        0,
    );
    assert_time_close(
        json.get("transit")
            .and_then(Value::as_str)
            .expect("missing transit"),
        "2024-06-21T11:08:18+00:00",
        0,
    );
    assert_time_close(
        json.get("sunset")
            .and_then(Value::as_str)
            .expect("missing sunset"),
        "2024-06-21T19:30:20+00:00",
        0,
    );
    assert_time_close(
        json.get("civil_start")
            .and_then(Value::as_str)
            .expect("missing civil_start"),
        "2024-06-21T01:57:19+00:00",
        0,
    );
    assert_time_close(
        json.get("civil_end")
            .and_then(Value::as_str)
            .expect("missing civil_end"),
        "2024-06-21T20:19:16+00:00",
        0,
    );
    assert_time_close(
        json.get("nautical_start")
            .and_then(Value::as_str)
            .expect("missing nautical_start"),
        "2024-06-21T00:38:45+00:00",
        0,
    );
    assert_time_close(
        json.get("nautical_end")
            .and_then(Value::as_str)
            .expect("missing nautical_end"),
        "2024-06-21T21:37:47+00:00",
        1,
    );
}

#[test]
fn test_twilight_csv_and_json_outputs() {
    let csv = twilight_csv_record(&[]);
    let csv_headers = parse_csv_output(&output_text(&[
        "--format=csv",
        "--timezone=UTC",
        "52.0",
        "13.4",
        "2024-06-21",
        "sunrise",
        "--twilight",
    ]))
    .0;
    assert_eq!(
        csv_headers,
        fields(&[
            "dateTime",
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
        ])
    );
    assert_eq!(
        csv.get("dateTime").map(String::as_str),
        Some("2024-06-21T00:00:00+00:00")
    );
    assert_eq!(csv.get("type").map(String::as_str), Some("NORMAL"));
    assert_core_twilight_times(&csv);
    assert_eq!(csv.get("astronomical_start").map(String::as_str), Some(""));
    assert_eq!(csv.get("astronomical_end").map(String::as_str), Some(""));

    let json = parse_json_output(&output_text(&[
        "--format=json",
        "--timezone=UTC",
        "52.0",
        "13.4",
        "2024-06-21",
        "sunrise",
        "--twilight",
    ]));
    assert_eq!(json.get("type").and_then(Value::as_str), Some("NORMAL"));
    assert_core_twilight_json_times(&json);
    assert!(json.get("astronomical_start").is_some_and(Value::is_null));
    assert!(json.get("astronomical_end").is_some_and(Value::is_null));
}

#[test]
fn test_twilight_text_and_column_presence() {
    let stdout = output_text(&[
        "--timezone=UTC",
        "52.0",
        "13.4",
        "2024-06-21",
        "sunrise",
        "--twilight",
    ]);
    for label in [
        "dateTime",
        "type",
        "NORMAL",
        "sunrise",
        "transit",
        "sunset",
        "civil_start",
        "civil_end",
        "nautical_start",
        "nautical_end",
    ] {
        assert!(stdout.contains(label));
    }

    let headers = parse_csv_output(&output_text(&[
        "--format=csv",
        "--timezone=UTC",
        "52.0",
        "13.4",
        "2024-06-20",
        "sunrise",
        "--twilight",
    ]))
    .0;
    assert!(headers.contains(&"civil_start".to_string()));
    assert!(headers.contains(&"astronomical_end".to_string()));
}

#[test]
fn test_twilight_without_show_inputs_and_without_flag() {
    let headers = parse_csv_output(&output_text(&[
        "--format=csv",
        "--no-show-inputs",
        "--timezone=UTC",
        "52.0",
        "13.4",
        "2024-06-21",
        "sunrise",
        "--twilight",
    ]))
    .0;
    assert_eq!(
        headers,
        fields(&[
            "dateTime",
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
        ])
    );

    let no_twilight_headers = parse_csv_output(&output_text(&[
        "--format=csv",
        "--timezone=UTC",
        "52.0",
        "13.4",
        "2024-06-21",
        "sunrise",
    ]))
    .0;
    assert_eq!(
        no_twilight_headers,
        fields(&["dateTime", "type", "sunrise", "transit", "sunset"])
    );
    for field in ["civil_start", "nautical_start", "astronomical_start"] {
        assert!(!no_twilight_headers.contains(&field.to_string()));
    }
}

#[test]
fn test_twilight_polar_json_behavior() {
    for (lat, lon, date, expected_type) in [
        ("80.0", "0.0", "2024-06-21", "ALL_DAY"),
        ("80.0", "0.0", "2024-12-21", "ALL_NIGHT"),
    ] {
        let json = parse_json_output(&output_text(&[
            "--format=json",
            "--timezone=UTC",
            lat,
            lon,
            date,
            "sunrise",
        ]));
        assert_eq!(
            json.get("type").and_then(Value::as_str),
            Some(expected_type)
        );
        assert!(json.get("sunrise").is_some_and(Value::is_null));
        assert!(json.get("sunset").is_some_and(Value::is_null));
    }

    let json = parse_json_output(&output_text(&[
        "--format=json",
        "--timezone=UTC",
        "78.0",
        "15.0",
        "2024-12-21",
        "sunrise",
        "--twilight",
    ]));
    assert!(json.get("type").and_then(Value::as_str).is_some());
    assert!(json.get("sunrise").is_some());
    assert!(json.get("sunset").is_some());
}

#[cfg(feature = "parquet")]
#[test]
fn test_twilight_parquet_output() {
    use arrow::array::StringArray;
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let output = SunceTest::new()
        .args([
            "--format=parquet",
            "--timezone=UTC",
            "52.0",
            "13.4",
            "2024-06-21",
            "sunrise",
            "--twilight",
        ])
        .get_output();

    assert!(output.status.success());
    let bytes = Bytes::from(output.stdout);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes).expect("Failed to create Parquet reader");
    assert_eq!(
        builder
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec![
            "dateTime",
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

    let batch = builder
        .build()
        .expect("Failed to build Parquet reader")
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(batch.num_rows(), 1);
    for (field, expected_prefix) in [
        ("sunrise", "2024-06-21T02:46:15"),
        ("civil_start", "2024-06-21T01:57:19"),
        ("nautical_start", "2024-06-21T00:38:45"),
    ] {
        let value = batch
            .column_by_name(field)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert!(value.contains(expected_prefix));
    }
}
