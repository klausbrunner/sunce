mod common;
use common::{csv_row_map, parse_csv_output, parse_json_output, sunce_command};
use serde_json::Value;

/// Test basic sunrise calculation
#[test]
fn test_basic_sunrise() {
    let mut cmd = sunce_command();
    cmd.args(["--format=JSON", "52.0", "13.4", "2024-06-21", "sunrise"]);

    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let json = parse_json_output(&stdout);
    assert!(json.get("sunrise").is_some());
    assert!(json.get("sunset").is_some());
}

/// Test sunrise output formats
#[test]
fn test_sunrise_output_formats() {
    // Test HUMAN format (default)
    let mut human = sunce_command();
    human.args(["52.0", "13.4", "2024-06-21", "sunrise"]);
    let human_output = human.output().unwrap();
    assert!(human_output.status.success());
    let human_stdout = String::from_utf8(human_output.stdout).unwrap();
    assert!(human_stdout.contains("sunrise"));
    assert!(human_stdout.contains("sunset"));

    // Test CSV format
    let mut csv = sunce_command();
    csv.args(["--format=CSV", "52.0", "13.4", "2024-06-21", "sunrise"]);
    let csv_output = csv.output().unwrap();
    assert!(csv_output.status.success());
    let csv_stdout = String::from_utf8(csv_output.stdout).unwrap();
    let (headers, rows) = parse_csv_output(&csv_stdout);
    assert_eq!(headers.len(), 5);
    assert!(headers.contains(&"type".to_string()));
    assert!(headers.contains(&"sunrise".to_string()));
    assert!(headers.contains(&"transit".to_string()));
    assert!(headers.contains(&"sunset".to_string()));
    assert_eq!(rows.len(), 1);

    // Test JSON format
    let mut json = sunce_command();
    json.args(["--format=JSON", "52.0", "13.4", "2024-06-21", "sunrise"]);
    let json_output = json.output().unwrap();
    assert!(json_output.status.success());
    let json_stdout = String::from_utf8(json_output.stdout).unwrap();
    let json_obj = parse_json_output(&json_stdout);
    assert!(json_obj.get("type").is_some());
    assert!(json_obj.get("sunrise").is_some());
}

/// Test sunrise edge cases (Arctic midnight sun and polar night)
#[test]
fn test_sunrise_edge_cases() {
    // Test Arctic summer (midnight sun)
    let mut summer = sunce_command();
    summer.args(["--format=JSON", "80.0", "0.0", "2024-06-21", "sunrise"]);
    let summer_output = summer.output().unwrap();
    assert!(summer_output.status.success());
    let summer_json = parse_json_output(&String::from_utf8(summer_output.stdout).unwrap());
    assert_eq!(
        summer_json.get("type").and_then(Value::as_str),
        Some("ALL_DAY")
    );

    // Test Arctic winter (polar night)
    let mut winter = sunce_command();
    winter.args(["--format=JSON", "80.0", "0.0", "2024-12-21", "sunrise"]);
    let winter_output = winter.output().unwrap();
    assert!(winter_output.status.success());
    let winter_json = parse_json_output(&String::from_utf8(winter_output.stdout).unwrap());
    assert_eq!(
        winter_json.get("type").and_then(Value::as_str),
        Some("ALL_NIGHT")
    );
}

/// Test sunrise time series
#[test]
fn test_sunrise_time_series() {
    let mut cmd = sunce_command();
    cmd.env("TZ", "Europe/Berlin")
        .args(["--format=CSV", "52.0", "13.4", "2024-06", "sunrise"]);

    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let output_str = String::from_utf8(output.stdout).unwrap();
    let (headers, rows) = parse_csv_output(&output_str);
    let datetimes = rows
        .iter()
        .map(|row| {
            let record = csv_row_map(&headers, row);
            record["dateTime"].clone()
        })
        .collect::<Vec<_>>();

    // Should have daily series for the entire month
    assert!(
        datetimes
            .iter()
            .any(|ts| ts.starts_with("2024-06-01T00:00:00+02:00"))
    );
    assert!(
        datetimes
            .iter()
            .any(|ts| ts.starts_with("2024-06-15T00:00:00+02:00"))
    );
    assert!(
        datetimes
            .iter()
            .any(|ts| ts.starts_with("2024-06-30T00:00:00+02:00"))
    );
}

/// Test sunrise coordinate ranges
#[test]
fn test_sunrise_coordinate_ranges() {
    let mut cmd = sunce_command();
    cmd.env("TZ", "Europe/Berlin").args([
        "--format=CSV",
        "52:53:1",
        "13:14:1",
        "2024-06-21",
        "sunrise",
    ]);

    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let output_str = String::from_utf8(output.stdout).unwrap();
    let (_headers, rows) = parse_csv_output(&output_str);

    // Should have 4 coordinate combinations (2x2 grid)
    assert_eq!(rows.len(), 4);
}

/// Test sunrise with timezone - verifies timezone is correctly applied to specific dates
#[test]
fn test_sunrise_timezone() {
    let mut cmd = sunce_command();
    cmd.args([
        "--format=JSON",
        "--timezone=+02:00",
        "52.0",
        "13.4",
        "2024-06-21",
        "sunrise",
    ]);
    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let json = parse_json_output(&String::from_utf8(output.stdout).unwrap());
    let sunrise = json.get("sunrise").and_then(Value::as_str).unwrap();
    let transit = json.get("transit").and_then(Value::as_str).unwrap();
    let sunset = json.get("sunset").and_then(Value::as_str).unwrap();
    assert!(sunrise.ends_with("+02:00"));
    assert!(transit.ends_with("+02:00"));
    assert!(sunset.ends_with("+02:00"));
}

/// Test timezone fix for specific dates - regression test for timezone bug
#[test]
fn test_sunrise_timezone_specific_date_fix() {
    // Test case 1: +02:00 timezone
    let mut plus_two = sunce_command();
    plus_two.args([
        "--format=CSV",
        "--timezone=+02:00",
        "52.0",
        "13.4",
        "2024-06-21",
        "sunrise",
    ]);

    let output = plus_two.output().unwrap();
    assert!(output.status.success());
    let output_str = String::from_utf8(output.stdout).unwrap();
    let (headers, rows) = parse_csv_output(&output_str);
    assert_eq!(rows.len(), 1, "Should have header + 1 data row");
    let record = csv_row_map(&headers, &rows[0]);
    assert!(record["sunrise"].ends_with("+02:00"));
    assert!(record["transit"].ends_with("+02:00"));
    assert!(record["sunset"].ends_with("+02:00"));

    // Test case 2: -05:00 timezone
    let mut minus_five = sunce_command();
    minus_five.args([
        "--format=CSV",
        "--timezone=-05:00",
        "40.7",
        "-74.0",
        "2024-12-21",
        "sunrise",
    ]);

    let output = minus_five.output().unwrap();
    assert!(output.status.success());
    let output_str = String::from_utf8(output.stdout).unwrap();
    let (headers, rows) = parse_csv_output(&output_str);
    assert_eq!(rows.len(), 1, "Should have header + 1 data row");
    let record = csv_row_map(&headers, &rows[0]);
    assert!(record["sunrise"].ends_with("-05:00"));
    assert!(record["transit"].ends_with("-05:00"));
    assert!(record["sunset"].ends_with("-05:00"));
}

/// Test sunrise validation
#[test]
fn test_sunrise_validation() {
    // Test invalid horizon
    let mut cmd = sunce_command();
    cmd.args(["52.0", "13.4", "2024-06-21", "sunrise", "--horizon=invalid"]);
    cmd.assert().failure();
}

/// Test sunrise show-inputs
#[test]
fn test_sunrise_show_inputs() {
    let mut cmd = sunce_command();
    cmd.env("TZ", "Europe/Berlin").args([
        "--format=CSV",
        "--show-inputs",
        "52.0",
        "13.4",
        "2024-06-21",
        "sunrise",
    ]);
    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let output_str = String::from_utf8(output.stdout).unwrap();
    let (headers, _rows) = parse_csv_output(&output_str);
    assert!(headers.contains(&"latitude".to_string()));
    assert!(headers.contains(&"longitude".to_string()));
}

/// Test sunrise DST handling
#[test]
fn test_sunrise_dst() {
    let mut cmd = sunce_command();
    cmd.env("TZ", "Europe/Berlin")
        .args(["52.0", "13.4", "2024-03-31", "sunrise"]);
    cmd.assert().success();
}

/// Test sunrise accuracy against known reference values to prevent horizon value regression
#[test]
fn test_sunrise_accuracy_regression() {
    // Test the specific case that revealed the Horizon::Custom(-0.833) vs Horizon::SunriseSunset bug
    let mut first = sunce_command();
    first.env("TZ", "Europe/Berlin").args([
        "--format=CSV",
        "53.0",
        "13.0",
        "2024-06-21",
        "sunrise",
    ]);

    let output = first.output().unwrap();
    assert!(output.status.success());
    let output_str = String::from_utf8(output.stdout).unwrap();
    let (headers, rows) = parse_csv_output(&output_str);
    let record = csv_row_map(&headers, &rows[0]);
    assert!(record["sunrise"].contains("T04:41:"));
    assert!(record["transit"].contains("T13:09:"));
    assert!(record["sunset"].contains("T21:37:"));
    assert!(record["sunrise"].ends_with("+02:00"));
    assert!(record["transit"].ends_with("+02:00"));
    assert!(record["sunset"].ends_with("+02:00"));

    // Test second problematic case that also revealed the bug
    let mut second = sunce_command();
    second.env("TZ", "Europe/Berlin").args([
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-06-02",
        "sunrise",
    ]);

    let output = second.output().unwrap();
    assert!(output.status.success());
    let output_str = String::from_utf8(output.stdout).unwrap();
    let (headers, rows) = parse_csv_output(&output_str);
    let record = csv_row_map(&headers, &rows[0]);
    assert!(record["sunrise"].contains("T04:51:"));
    assert!(record["transit"].contains("T13:04:"));
    assert!(record["sunset"].contains("T21:18:"));
    assert!(record["sunrise"].ends_with("+02:00"));
    assert!(record["transit"].ends_with("+02:00"));
    assert!(record["sunset"].ends_with("+02:00"));
}

/// Test sunrise date parsing bug fix: specific dates should produce single results
#[test]
fn test_sunrise_specific_date_single_result() {
    let mut cmd = sunce_command();
    cmd.args(["--format=CSV", "52.0", "13.4", "2024-01-01", "sunrise"]);

    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let output_str = String::from_utf8(output.stdout).unwrap();
    let (headers, rows) = parse_csv_output(&output_str);

    // Should be exactly 1 data row
    assert_eq!(rows.len(), 1, "Expected exactly 1 data row");
    assert!(headers.contains(&"type".to_string()));
    assert!(headers.contains(&"sunrise".to_string()));
    assert!(headers.contains(&"sunset".to_string()));

    let record = csv_row_map(&headers, &rows[0]);
    assert_eq!(record.get("type"), Some(&"NORMAL".to_string()));
}

/// Test that partial dates generate daily time series for sunrise
/// Regression test: Sunrise times don't change hourly, must be daily
#[test]
fn test_sunrise_partial_date_time_series() {
    let mut cmd = sunce_command();
    cmd.args(["--format=CSV", "52.0", "13.4", "2024-01", "sunrise"]);

    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let output_str = String::from_utf8(output.stdout).unwrap();
    let (headers, rows) = parse_csv_output(&output_str);

    // Should be exactly 31 daily data rows (not hourly)
    assert_eq!(rows.len(), 31, "Expected 31 daily rows for January");
    assert!(headers.contains(&"latitude".to_string()));
    assert!(headers.contains(&"longitude".to_string()));
    assert!(headers.contains(&"dateTime".to_string()));

    let first = csv_row_map(&headers, &rows[0]);
    let last = csv_row_map(&headers, &rows[30]);
    assert_eq!(first.get("latitude"), Some(&"52.00000".to_string()));
    assert_eq!(first.get("longitude"), Some(&"13.40000".to_string()));
    assert!(first["dateTime"].starts_with("2024-01-01"));
    assert!(last["dateTime"].starts_with("2024-01-31"));
}

/// Test position command still generates time series for specific dates (expected behavior)
#[test]
fn test_position_specific_date_time_series() {
    let mut cmd = sunce_command();
    cmd.args(["--format=CSV", "52.0", "13.4", "2024-01-01", "position"]);

    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let output_str = String::from_utf8(output.stdout).unwrap();
    let (headers, rows) = parse_csv_output(&output_str);

    // Should be 24 hourly data rows
    assert_eq!(rows.len(), 24, "Expected 24 hourly rows");
    assert!(headers.contains(&"latitude".to_string()));
    assert!(headers.contains(&"longitude".to_string()));
    assert!(headers.contains(&"dateTime".to_string()));
    assert!(headers.contains(&"azimuth".to_string()));
    assert!(headers.contains(&"zenith".to_string()));
}
