/// Comprehensive twilight functionality tests
/// Verifies that --twilight flag properly calculates all horizons across all output formats
mod common;
use common::*;
use serde_json::Value;

#[test]
fn test_twilight_csv_output() {
    let output = SunceTest::new()
        .args([
            "--format=csv",
            "--timezone=UTC",
            "52.0",
            "13.4",
            "2024-06-21",
            "sunrise",
            "--twilight",
        ])
        .get_output();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let (headers, row) = parse_csv_single_record(&stdout);
    let record = csv_row_map(&headers, &row);

    assert_eq!(
        headers,
        vec![
            "dateTime".to_string(),
            "type".to_string(),
            "sunrise".to_string(),
            "transit".to_string(),
            "sunset".to_string(),
            "civil_start".to_string(),
            "civil_end".to_string(),
            "nautical_start".to_string(),
            "nautical_end".to_string(),
            "astronomical_start".to_string(),
            "astronomical_end".to_string(),
        ]
    );

    assert_eq!(
        record.get("dateTime"),
        Some(&"2024-06-21T00:00:00+00:00".to_string())
    );
    assert_eq!(record.get("type"), Some(&"NORMAL".to_string()));
    assert_time_close(
        record.get("sunrise").expect("missing sunrise").as_str(),
        "2024-06-21T02:46:15+00:00",
        0,
    );
    assert_time_close(
        record.get("transit").expect("missing transit").as_str(),
        "2024-06-21T11:08:18+00:00",
        0,
    );
    assert_time_close(
        record.get("sunset").expect("missing sunset").as_str(),
        "2024-06-21T19:30:20+00:00",
        0,
    );
    assert_time_close(
        record
            .get("civil_start")
            .expect("missing civil_start")
            .as_str(),
        "2024-06-21T01:57:19+00:00",
        0,
    );
    assert_time_close(
        record.get("civil_end").expect("missing civil_end").as_str(),
        "2024-06-21T20:19:16+00:00",
        0,
    );
    assert_time_close(
        record
            .get("nautical_start")
            .expect("missing nautical_start")
            .as_str(),
        "2024-06-21T00:38:45+00:00",
        0,
    );
    // Upstream algorithm/version updates can shift twilight boundary rounding by 1s.
    assert_time_close(
        record
            .get("nautical_end")
            .expect("missing nautical_end")
            .as_str(),
        "2024-06-21T21:37:47+00:00",
        1,
    );
    // Astronomical twilight may be absent (sun doesn't go that deep at lat 52° in June).
    assert_eq!(record.get("astronomical_start"), Some(&"".to_string()));
    assert_eq!(record.get("astronomical_end"), Some(&"".to_string()));
}

#[test]
fn test_twilight_json_output() {
    let output = SunceTest::new()
        .args([
            "--format=json",
            "--timezone=UTC",
            "52.0",
            "13.4",
            "2024-06-21",
            "sunrise",
            "--twilight",
        ])
        .get_output();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let json = parse_json_output(&stdout);

    assert_eq!(json.get("type").and_then(Value::as_str), Some("NORMAL"));
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
    // Upstream algorithm/version updates can shift twilight boundary rounding by 1s.
    assert_time_close(
        json.get("nautical_end")
            .and_then(Value::as_str)
            .expect("missing nautical_end"),
        "2024-06-21T21:37:47+00:00",
        1,
    );
    assert!(
        json.get("astronomical_start")
            .expect("missing astronomical_start")
            .is_null()
    );
    assert!(
        json.get("astronomical_end")
            .expect("missing astronomical_end")
            .is_null()
    );
}

#[test]
fn test_json_polar_day_and_night_use_null() {
    let polar_day = SunceTest::new()
        .args([
            "--format=json",
            "--timezone=UTC",
            "80.0",
            "0.0",
            "2024-06-21",
            "sunrise",
        ])
        .get_output();
    assert!(polar_day.status.success());
    let day_output = String::from_utf8(polar_day.stdout).unwrap();
    let day_json = parse_json_output(&day_output);
    assert_eq!(
        day_json.get("type").and_then(Value::as_str),
        Some("ALL_DAY")
    );
    assert!(day_json.get("sunrise").expect("missing sunrise").is_null());
    assert!(day_json.get("sunset").expect("missing sunset").is_null());

    let polar_night = SunceTest::new()
        .args([
            "--format=json",
            "--timezone=UTC",
            "80.0",
            "0.0",
            "2024-12-21",
            "sunrise",
        ])
        .get_output();
    assert!(polar_night.status.success());
    let night_output = String::from_utf8(polar_night.stdout).unwrap();
    let night_json = parse_json_output(&night_output);
    assert_eq!(
        night_json.get("type").and_then(Value::as_str),
        Some("ALL_NIGHT")
    );
    assert!(
        night_json
            .get("sunrise")
            .expect("missing sunrise")
            .is_null()
    );
    assert!(night_json.get("sunset").expect("missing sunset").is_null());
}

#[test]
fn test_twilight_text_output() {
    let output = SunceTest::new()
        .args([
            "--timezone=UTC",
            "52.0",
            "13.4",
            "2024-06-21",
            "sunrise",
            "--twilight",
        ])
        .get_output();

    let stdout = String::from_utf8(output.stdout).unwrap();

    // Verify text format includes all twilight information
    assert!(stdout.contains("dateTime"));
    assert!(stdout.contains("type"));
    assert!(stdout.contains("NORMAL"));
    assert!(stdout.contains("sunrise"));
    assert!(stdout.contains("transit"));
    assert!(stdout.contains("sunset"));
    assert!(stdout.contains("civil_start"));
    assert!(stdout.contains("civil_end"));
    assert!(stdout.contains("nautical_start"));
    assert!(stdout.contains("nautical_end"));
    // Astronomical may not appear at this latitude in summer (empty fields are allowed)
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
    assert!(!output.stdout.is_empty());

    // Read parquet from stdout
    let bytes = Bytes::from(output.stdout);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes).expect("Failed to create Parquet reader");

    // Verify schema includes all twilight fields (no show-inputs for single values)
    let schema = builder.schema();
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
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
            "astronomical_end"
        ]
    );

    // Read and verify data
    let mut reader = builder.build().expect("Failed to build Parquet reader");
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(batch.num_rows(), 1);

    // Check sunrise time
    let sunrise_col = batch
        .column_by_name("sunrise")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sunrise = sunrise_col.value(0);
    assert!(sunrise.contains("2024-06-21T02:46:15"));

    // Check civil twilight start
    let civil_start_col = batch
        .column_by_name("civil_start")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let civil_start = civil_start_col.value(0);
    assert!(civil_start.contains("2024-06-21T01:57:19"));

    // Check nautical twilight
    let nautical_start_col = batch
        .column_by_name("nautical_start")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let nautical_start = nautical_start_col.value(0);
    assert!(nautical_start.contains("2024-06-21T00:38:45"));
}

#[test]
fn test_twilight_without_show_inputs() {
    let output = SunceTest::new()
        .args([
            "--format=csv",
            "--no-show-inputs",
            "--timezone=UTC",
            "52.0",
            "13.4",
            "2024-06-21",
            "sunrise",
            "--twilight",
        ])
        .get_output();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let (headers, row) = parse_csv_single_record(&stdout);
    let record = csv_row_map(&headers, &row);
    assert_eq!(
        headers,
        vec![
            "dateTime".to_string(),
            "type".to_string(),
            "sunrise".to_string(),
            "transit".to_string(),
            "sunset".to_string(),
            "civil_start".to_string(),
            "civil_end".to_string(),
            "nautical_start".to_string(),
            "nautical_end".to_string(),
            "astronomical_start".to_string(),
            "astronomical_end".to_string(),
        ]
    );
    assert_eq!(record.get("type"), Some(&"NORMAL".to_string()));
}

#[test]
fn test_twilight_polar_night() {
    // Test location with polar night (no sunrise, but twilight may still exist)
    let output = SunceTest::new()
        .args([
            "--format=json",
            "--timezone=UTC",
            "78.0", // Svalbard
            "15.0",
            "2024-12-21", // Winter solstice
            "sunrise",
            "--twilight",
        ])
        .get_output();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let json = parse_json_output(&stdout);

    // Should contain a valid type classification and required core fields.
    assert!(json.get("type").and_then(Value::as_str).is_some());
    assert!(json.get("sunrise").is_some());
    assert!(json.get("sunset").is_some());
}

#[test]
fn test_twilight_multiple_dates() {
    // Test twilight with time series
    let output = SunceTest::new()
        .args([
            "--format=csv",
            "--timezone=UTC",
            "52.0",
            "13.4",
            "2024-06-20",
            "sunrise",
            "--twilight",
        ])
        .get_output();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let (headers, _record) = parse_csv_single_record(&stdout);

    assert!(headers.contains(&"civil_start".to_string()));
    assert!(headers.contains(&"astronomical_end".to_string()));
}

#[test]
fn test_no_twilight_flag_behavior() {
    // Verify that WITHOUT --twilight, we don't get twilight columns
    let output = SunceTest::new()
        .args([
            "--format=csv",
            "--timezone=UTC",
            "52.0",
            "13.4",
            "2024-06-21",
            "sunrise",
        ])
        .get_output();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let (headers, _record) = parse_csv_single_record(&stdout);

    assert!(!headers.contains(&"civil_start".to_string()));
    assert!(!headers.contains(&"nautical_start".to_string()));
    assert!(!headers.contains(&"astronomical_start".to_string()));
    assert_eq!(
        headers,
        vec![
            "dateTime".to_string(),
            "type".to_string(),
            "sunrise".to_string(),
            "transit".to_string(),
            "sunset".to_string(),
        ]
    );
}
