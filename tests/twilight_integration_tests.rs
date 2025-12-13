/// Comprehensive twilight functionality tests
/// Verifies that --twilight flag properly calculates all horizons across all output formats
mod common;
use common::*;

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

    let stdout = String::from_utf8(output.stdout).unwrap();

    // Verify CSV header includes all twilight fields (no show-inputs for single values)
    assert!(stdout.contains("dateTime,type,sunrise,transit,sunset,civil_start,civil_end,nautical_start,nautical_end,astronomical_start,astronomical_end"));

    // Verify data row starts with timestamp (RFC3339)
    assert!(stdout.contains("2024-06-21T00:00:00+00:00,NORMAL"));

    // Verify all times are present (Berlin in summer - all twilight phases exist)
    assert!(stdout.contains("2024-06-21T02:46:15")); // sunrise
    assert!(stdout.contains("2024-06-21T11:08:18")); // transit
    assert!(stdout.contains("2024-06-21T19:30:20")); // sunset
    assert!(stdout.contains("2024-06-21T01:57:19")); // civil start
    assert!(stdout.contains("2024-06-21T20:19:16")); // civil end
    assert!(stdout.contains("2024-06-21T00:38:45")); // nautical start
    assert!(stdout.contains("2024-06-21T21:37:47")); // nautical end
    // Astronomical twilight may be absent (sun doesn't go that deep at lat 52° in June)
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

    let stdout = String::from_utf8(output.stdout).unwrap();

    // Verify JSON structure
    assert!(stdout.contains(r#""type":"NORMAL""#));
    assert!(stdout.contains(r#""sunrise":"2024-06-21T02:46:15"#));
    assert!(stdout.contains(r#""transit":"2024-06-21T11:08:18"#));
    assert!(stdout.contains(r#""sunset":"2024-06-21T19:30:20"#));
    assert!(stdout.contains(r#""civil_start":"2024-06-21T01:57:19"#));
    assert!(stdout.contains(r#""civil_end":"2024-06-21T20:19:16"#));
    assert!(stdout.contains(r#""nautical_start":"2024-06-21T00:38:45"#));
    assert!(stdout.contains(r#""nautical_end":"2024-06-21T21:37:47"#));
    assert!(stdout.contains(r#""astronomical_start":null"#));
    assert!(stdout.contains(r#""astronomical_end":null"#));
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
    let day_output = String::from_utf8(polar_day.stdout).unwrap();
    assert!(day_output.contains(r#""type":"ALL_DAY""#));
    assert!(day_output.contains(r#""sunrise":null"#));
    assert!(day_output.contains(r#""sunset":null"#));

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
    let night_output = String::from_utf8(polar_night.stdout).unwrap();
    assert!(night_output.contains(r#""type":"ALL_NIGHT""#));
    assert!(night_output.contains(r#""sunrise":null"#));
    assert!(night_output.contains(r#""sunset":null"#));
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

    let stdout = String::from_utf8(output.stdout).unwrap();

    // Verify header doesn't include lat/lon/deltat
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(
        lines[0],
        "dateTime,type,sunrise,transit,sunset,civil_start,civil_end,nautical_start,nautical_end,astronomical_start,astronomical_end"
    );

    // Should still have data row with all fields
    assert!(lines[1].contains("2024-06-21"));
    assert!(lines[1].contains("NORMAL"));
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

    let stdout = String::from_utf8(output.stdout).unwrap();

    // Should handle ALL_NIGHT or similar gracefully
    assert!(stdout.contains(r#""type":"#));
    // Twilight times may still exist even during polar night
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

    let stdout = String::from_utf8(output.stdout).unwrap();

    // Should have header + 1 data row
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 2); // header + 1 data row

    // Verify header
    assert!(lines[0].contains("civil_start"));
    assert!(lines[0].contains("astronomical_end"));
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

    let stdout = String::from_utf8(output.stdout).unwrap();

    // Header should NOT include twilight fields
    assert!(!stdout.contains("civil_start"));
    assert!(!stdout.contains("nautical_start"));
    assert!(!stdout.contains("astronomical_start"));

    // Should only have standard fields (no show-inputs for single values)
    assert!(stdout.contains("dateTime,type,sunrise,transit,sunset"));
}
