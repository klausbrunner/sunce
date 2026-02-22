mod common;
use common::*;
use predicates::prelude::*;

/// Test basic position calculation
#[test]
fn test_basic_position_calculation() {
    position_test().assert_success_contains_all(&["dateTime", "azimuth", "zenith"]);
}

/// Test position with different algorithms
#[test]
fn test_position_algorithms() {
    // Test SPA algorithm
    position_test().arg("--algorithm=SPA").assert_success();

    // Test GRENA3 algorithm
    position_test().arg("--algorithm=GRENA3").assert_success();
}

/// Test different output formats
#[test]
fn test_output_formats() {
    // Test HUMAN format (default) - table format
    position_test().assert_success_contains("azimuth");

    // Test CSV format
    let csv_output = position_test_with_format("CSV").get_output();
    assert!(csv_output.status.success());
    let csv_stdout = String::from_utf8(csv_output.stdout).unwrap();
    let (headers, rows) = parse_csv_output(&csv_stdout);
    assert_eq!(
        headers,
        vec![
            "dateTime".to_string(),
            "azimuth".to_string(),
            "zenith".to_string()
        ]
    );
    assert_eq!(rows.len(), 1);

    // Test JSON format
    let json_output = position_test_with_format("JSON").get_output();
    assert!(json_output.status.success());
    let json_stdout = String::from_utf8(json_output.stdout).unwrap();
    let json = parse_json_output(&json_stdout);
    assert!(json.get("dateTime").is_some());
    assert!(json.get("azimuth").is_some());

    // Test PARQUET format (only when feature enabled)
    #[cfg(feature = "parquet")]
    {
        let output = position_test_with_format("PARQUET").get_output();
        assert!(output.status.success());
        // Parquet is binary format, so we can't check string content
        // Just verify the command succeeded and produced output
        assert!(!output.stdout.is_empty());
    }

    // Test PARQUET format rejection (only when feature disabled)
    #[cfg(not(feature = "parquet"))]
    {
        let output = position_test_with_format("PARQUET").get_output();
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("PARQUET format not available in this build"));
    }
}

/// Test elevation angle vs zenith angle
#[test]
fn test_elevation_vs_zenith() {
    // Test default (zenith angle)
    let default_output = position_test_with_format("CSV").get_output();
    assert!(default_output.status.success());
    let default_stdout = String::from_utf8(default_output.stdout).unwrap();
    let (default_headers, _rows) = parse_csv_output(&default_stdout);
    assert!(default_headers.contains(&"zenith".to_string()));

    // Test elevation angle
    let elevation_output = position_test_with_elevation().get_output();
    assert!(elevation_output.status.success());
    let elevation_stdout = String::from_utf8(elevation_output.stdout).unwrap();
    let (elevation_headers, _rows) = parse_csv_output(&elevation_stdout);
    assert!(elevation_headers.contains(&"elevation-angle".to_string()));
}

/// Test coordinate ranges (geographic sweeps)
#[test]
fn test_coordinate_ranges() {
    let output = coordinate_range_test()
        .command()
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should have 4 coordinate combinations (2x2 grid)
    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 5); // Header + 4 data rows

    let (headers, rows) = parse_csv_output(&output_str);
    let combos = rows
        .iter()
        .map(|row| {
            let record = csv_row_map(&headers, row);
            (record["latitude"].clone(), record["longitude"].clone())
        })
        .collect::<std::collections::HashSet<_>>();
    assert!(combos.contains(&("52.00000".to_string(), "13.00000".to_string())));
    assert!(combos.contains(&("52.00000".to_string(), "14.00000".to_string())));
    assert!(combos.contains(&("53.00000".to_string(), "13.00000".to_string())));
    assert!(combos.contains(&("53.00000".to_string(), "14.00000".to_string())));
}

/// Test time series generation
#[test]
fn test_time_series() {
    let output = time_series_test("2024-01-01", "6h")
        .command()
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let output_str = String::from_utf8(output).unwrap();

    let (headers, rows) = parse_csv_output(&output_str);
    let datetimes = rows
        .iter()
        .map(|row| {
            let record = csv_row_map(&headers, row);
            record["dateTime"].clone()
        })
        .collect::<std::collections::HashSet<_>>();
    assert!(
        datetimes
            .iter()
            .any(|ts| ts.starts_with("2024-01-01T00:00:00"))
    );
    assert!(
        datetimes
            .iter()
            .any(|ts| ts.starts_with("2024-01-01T06:00:00"))
    );
    assert!(
        datetimes
            .iter()
            .any(|ts| ts.starts_with("2024-01-01T12:00:00"))
    );
    assert!(
        datetimes
            .iter()
            .any(|ts| ts.starts_with("2024-01-01T18:00:00"))
    );
}

/// Test partial date inputs
#[test]
fn test_partial_dates() {
    // Test year input
    let output = time_series_test("2024", "24h")
        .command()
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let output_str = String::from_utf8(output).unwrap();
    let (headers, rows) = parse_csv_output(&output_str);
    let datetimes = rows
        .iter()
        .map(|row| {
            let record = csv_row_map(&headers, row);
            record["dateTime"].clone()
        })
        .collect::<Vec<_>>();
    assert!(
        datetimes
            .iter()
            .any(|ts| ts.starts_with("2024-01-01T00:00:00"))
    );
    assert!(
        datetimes
            .iter()
            .any(|ts| ts.starts_with("2024-12-31T00:00:00"))
    );

    // Test year-month input
    let output = time_series_test("2024-06", "24h")
        .command()
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let output_str = String::from_utf8(output).unwrap();
    let (headers, rows) = parse_csv_output(&output_str);
    let datetimes = rows
        .iter()
        .map(|row| {
            let record = csv_row_map(&headers, row);
            record["dateTime"].clone()
        })
        .collect::<Vec<_>>();
    assert!(
        datetimes
            .iter()
            .any(|ts| ts.starts_with("2024-06-01T00:00:00"))
    );
    assert!(
        datetimes
            .iter()
            .any(|ts| ts.starts_with("2024-06-30T00:00:00"))
    );
}

/// Test show-inputs functionality
#[test]
fn test_show_inputs() {
    // Test auto-enabling for ranges
    let auto_output = show_inputs_lat_range_test().get_output();
    assert!(auto_output.status.success());
    let auto_stdout = String::from_utf8(auto_output.stdout).unwrap();
    let (auto_headers, _rows) = parse_csv_output(&auto_stdout);
    assert_eq!(
        auto_headers,
        vec![
            "latitude".to_string(),
            "longitude".to_string(),
            "elevation".to_string(),
            "pressure".to_string(),
            "temperature".to_string(),
            "dateTime".to_string(),
            "deltaT".to_string(),
            "azimuth".to_string(),
            "zenith".to_string(),
        ]
    );

    // Test explicit disable
    let disabled_output = show_inputs_disabled_test().get_output();
    assert!(disabled_output.status.success());
    let disabled_stdout = String::from_utf8(disabled_output.stdout).unwrap();
    let (disabled_headers, _rows) = parse_csv_output(&disabled_stdout);
    assert_eq!(
        disabled_headers,
        vec![
            "dateTime".to_string(),
            "azimuth".to_string(),
            "zenith".to_string()
        ]
    );
}

/// Test environmental parameters
#[test]
fn test_environmental_parameters() {
    let output = environmental_params_test()
        .command()
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let output_str = String::from_utf8(output).unwrap();

    let (headers, rows) = parse_csv_output(&output_str);
    let record = csv_row_map(&headers, &rows[0]);
    assert_eq!(record.get("elevation"), Some(&"1000.000".to_string()));
    assert_eq!(record.get("pressure"), Some(&"900.000".to_string()));
    assert_eq!(record.get("temperature"), Some(&"25.000".to_string()));
}

/// Test refraction correction
#[test]
fn test_refraction_correction() {
    // Test with refraction (default)
    let output1 = position_test()
        .command()
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    // Test without refraction
    let output2 = position_no_refraction_test()
        .command()
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    // Results should be slightly different due to refraction correction
    assert_ne!(output1, output2);
}

/// Test timezone handling
#[test]
fn test_timezone_handling() {
    // Test with timezone override
    position_with_timezone("+02:00").assert_success_contains("+02:00");

    // Test with named timezone
    position_with_timezone("UTC").assert_success_contains("+00:00");
}

/// Test different time step formats
#[test]
fn test_time_step_formats() {
    // Test seconds
    time_series_test("2024-01-01", "30s").assert_success();

    // Test minutes
    time_series_test("2024-01-01", "15m").assert_success();

    // Test hours
    time_series_test("2024-01-01", "2h").assert_success();

    // Test days
    time_series_test("2024-01", "7d").assert_success();
}

/// Test coordinate validation
#[test]
fn test_coordinate_validation() {
    // Test invalid latitude
    custom_position("91.0", "13.4", "2024-01-01T12:00:00")
        .assert_failure()
        .stderr(predicate::str::contains("Latitude must be between"));

    // Test invalid longitude
    custom_position("52.0", "181.0", "2024-01-01T12:00:00")
        .assert_failure()
        .stderr(predicate::str::contains("Longitude must be between"));
}

/// Test datetime parsing
#[test]
fn test_datetime_parsing() {
    // Test various datetime formats
    let formats = [
        "2024-01-01T12:00:00",
        "2024-01-01 12:00:00",
        "2024-01-01T12:00:00Z",
        "2024-01-01T12:00:00+01:00",
    ];

    for format in &formats {
        custom_position("52.0", "13.4", format).assert_success();
    }
}

#[test]
fn test_space_separated_datetime_without_seconds() {
    let output = sunce_command()
        .env("TZ", "UTC")
        .args([
            "--format=JSON",
            "--no-show-inputs",
            "0",
            "0",
            "2024-01-01 12:00",
            "position",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "Command failed: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let json = parse_json_output(&stdout);
    assert_eq!(
        json.get("dateTime").and_then(serde_json::Value::as_str),
        Some("2024-01-01T12:00:00+00:00")
    );
}

/// Test edge cases
#[test]
fn test_edge_cases() {
    // Test North Pole
    custom_position("90.0", "0.0", "2024-06-21T12:00:00").assert_success();

    // Test South Pole
    custom_position("-90.0", "0.0", "2024-12-21T12:00:00").assert_success();

    // Test International Date Line
    custom_position("0.0", "180.0", "2024-01-01T12:00:00").assert_success();
}

/// Test combined range and time series
#[test]
fn test_combined_range_and_time_series() {
    let output = combined_range_time_test()
        .command()
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let output_str = String::from_utf8(output).unwrap();

    // Should have cartesian product: 2 lats × 2 lons × 2 times (00:00, 12:00) = 8 rows + header
    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 9); // Header + 8 data rows
}

/// Test now datetime
#[test]
fn test_now_datetime() {
    custom_position("52.0", "13.4", "now").assert_success();
}

/// Test now timestamp consistency with coordinate ranges
/// Verifies that multiple calculations within one command use the same "now" timestamp
/// This tests the OnceLock fix - without it, each calculation would get a different timestamp
#[test]
fn test_now_timestamp_consistency() {
    let output = SunceTest::new()
        .args([
            "--format=CSV",
            "--show-inputs",
            "52:53:1", // Generate 2 coordinate values (52, 53)
            "13.4",
            "now",
            "position",
        ])
        .get_output();

    let stdout = String::from_utf8(output.stdout).unwrap();
    let lines: Vec<&str> = stdout.lines().collect();

    // Should have header + 2 data lines (one for each coordinate)
    assert_eq!(lines.len(), 3, "Should have header + 2 data rows");

    // Extract timestamps from both data lines
    let mut timestamps = Vec::new();
    for line in lines.iter().skip(1) {
        // Skip header
        if line.contains(',') {
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() >= 6 {
                timestamps.push(parts[5]); // dateTime field
            }
        }
    }

    assert_eq!(timestamps.len(), 2, "Should have extracted 2 timestamps");

    // Both timestamps should be identical because they use the same "now"
    assert_eq!(
        timestamps[0], timestamps[1],
        "All calculations using 'now' should have identical timestamps. Found: {} and {}",
        timestamps[0], timestamps[1]
    );
}

/// Test headers in CSV output
#[test]
fn test_csv_headers() {
    // Test with headers (default)
    let with_headers = position_test_with_format("CSV").get_output();
    assert!(with_headers.status.success());
    let with_headers_stdout = String::from_utf8(with_headers.stdout).unwrap();
    let (headers, _rows) = parse_csv_output(&with_headers_stdout);
    assert_eq!(
        headers,
        vec![
            "dateTime".to_string(),
            "azimuth".to_string(),
            "zenith".to_string()
        ]
    );

    // Test without headers
    let output = position_csv_no_headers()
        .command()
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let output_str = String::from_utf8(output).unwrap();
    assert!(!output_str.contains("dateTime,azimuth,zenith"));
}

/// Test delta T parameter
#[test]
fn test_delta_t() {
    // Test with explicit delta T
    let explicit_output = position_with_deltat("69.2").get_output();
    assert!(explicit_output.status.success());
    let explicit_stdout = String::from_utf8(explicit_output.stdout).unwrap();
    let (headers, rows) = parse_csv_output(&explicit_stdout);
    let record = csv_row_map(&headers, &rows[0]);
    assert_eq!(record.get("deltaT"), Some(&"69.200".to_string()));

    // Test with delta T estimation
    position_with_deltat_estimation().assert_success();
}

/// Test error handling
#[test]
fn test_error_handling() {
    // Test missing arguments
    missing_args_test().assert_failure();

    // Test invalid time step
    invalid_step_test().assert_failure();

    // Test invalid algorithm
    invalid_algorithm_test().assert_failure();
}

/// Test unix timestamp support
#[test]
fn test_unix_timestamp_basic() {
    // Test basic unix timestamp (2020-01-01 00:00:00 UTC)
    SunceTest::new()
        .args(["--show-inputs", "52.0", "13.4", "1577836800", "position"])
        .assert_success_contains_all(&["2020-01-01T00:00:00", "azimuth", "zenith"]);
}

/// Test unix timestamp with timezone
#[test]
fn test_unix_timestamp_with_timezone() {
    // Test unix timestamp with timezone override
    SunceTest::new()
        .args([
            "--timezone=+01:00",
            "--show-inputs",
            "52.0",
            "13.4",
            "1577836800",
            "position",
        ])
        .assert_success_contains("2020-01-01T01:00:00+01:00");

    // Test with named timezone
    SunceTest::new()
        .args([
            "--timezone=Europe/Berlin",
            "--show-inputs",
            "52.0",
            "13.4",
            "1577836800",
            "position",
        ])
        .assert_success_contains("2020-01-01T01:00:00+01:00");
}

/// Test unix timestamp range validation
#[test]
fn test_unix_timestamp_validation() {
    // Test common timestamp (2000-01-01 = 946684800)
    SunceTest::new()
        .args(["--show-inputs", "52.0", "13.4", "946684800", "position"])
        .assert_success_contains("2000-01-01");

    // Test another valid timestamp (2020-01-01)
    SunceTest::new()
        .args(["--show-inputs", "52.0", "13.4", "1577836800", "position"])
        .assert_success_contains("2020-01-01");

    // Test boundary: 4 digits treated as year, 5+ as timestamp
    SunceTest::new()
        .args([
            "--format=csv",
            "--no-headers",
            "52.0",
            "13.4",
            "9999",
            "position",
        ])
        .assert_success_contains("9999-01-01");

    SunceTest::new()
        .args([
            "--format=csv",
            "--no-headers",
            "52.0",
            "13.4",
            "10000",
            "position",
        ])
        .assert_success_contains("1970-01-01T02:46:40");

    // Test negative timestamp (before epoch)
    SunceTest::new()
        .args([
            "--format=csv",
            "--no-headers",
            "52.0",
            "13.4",
            "-10000",
            "position",
        ])
        .assert_success_contains("1969-12-31");
}

/// Test unix timestamp in files
#[test]
fn test_unix_timestamp_in_files() {
    use std::fs;
    use tempfile::NamedTempFile;

    // Create temporary file with unix timestamp
    let file = NamedTempFile::new().unwrap();
    fs::write(&file, "52.0 13.4 1577836800\n").unwrap();

    // Test paired file with unix timestamp
    SunceTest::new()
        .args([&format!("@{}", file.path().display()), "position"])
        .assert_success_contains_all(&["2020-01-01", "52", "13.4"]);
}

/// Test floating point precision in coordinate ranges
/// Verifies that coordinate ranges with small steps don't miss endpoints due to floating point precision
#[test]
fn test_coordinate_range_floating_point_precision() {
    // Test small decimal steps that are prone to floating point precision issues
    let output = SunceTest::new()
        .args([
            "--format=CSV",
            "50.0:50.2:0.1", // This should give exactly 3 values: 50.0, 50.1, 50.2
            "10.0",
            "2024-01-01T12:00:00",
            "position",
        ])
        .get_output();

    let stdout = String::from_utf8(output.stdout).unwrap();
    let (headers, rows) = parse_csv_output(&stdout);

    // Should have 3 data rows (50.0, 50.1, 50.2)
    assert_eq!(rows.len(), 3, "Should have exactly 3 coordinate values");
    let latitudes = rows
        .iter()
        .map(|row| {
            let record = csv_row_map(&headers, row);
            record["latitude"].clone()
        })
        .collect::<std::collections::HashSet<_>>();
    assert!(latitudes.contains("50.00000"), "Should contain 50.0");
    assert!(latitudes.contains("50.10000"), "Should contain 50.1");
    assert!(latitudes.contains("50.20000"), "Should contain 50.2");

    // Test another problematic case with 0.3 step
    let output2 = SunceTest::new()
        .args([
            "--format=CSV",
            "0.0:0.9:0.3", // This should give exactly 4 values: 0.0, 0.3, 0.6, 0.9
            "0.0",
            "2024-01-01T12:00:00",
            "position",
        ])
        .get_output();

    let stdout2 = String::from_utf8(output2.stdout).unwrap();
    let (_headers2, rows2) = parse_csv_output(&stdout2);

    // Should have 4 data rows
    assert_eq!(
        rows2.len(),
        4,
        "Should have exactly 4 coordinate values for 0.3 step"
    );
}

/// Test watch mode with 'now' and --step
/// Verifies that watch mode generates multiple timestamped results at regular intervals
#[test]
fn test_watch_mode() {
    use std::process::{Command, Stdio};
    use std::thread;
    use std::time::Duration;

    let mut child = Command::new(assert_cmd::cargo::cargo_bin!("sunce"));
    let mut child = child
        .args([
            "--format=csv",
            "52.5",
            "13.4",
            "now",
            "position",
            "--step=1s",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to spawn watch mode process");

    thread::sleep(Duration::from_secs(3));

    child.kill().expect("Failed to kill watch mode process");
    let output = child.wait_with_output().expect("Failed to get output");

    let stdout = String::from_utf8(output.stdout).unwrap();
    let lines: Vec<&str> = stdout.lines().collect();

    // Should have header + at least 2 data rows (we slept for 3 seconds with 1s step)
    assert!(
        lines.len() >= 3,
        "Watch mode should produce at least 2 results in 3 seconds, got {} lines",
        lines.len()
    );

    assert_eq!(
        lines[0], "dateTime,azimuth,zenith",
        "Should have CSV header"
    );

    for (i, line) in lines.iter().skip(1).enumerate() {
        assert!(
            line.contains(','),
            "Data row {} should be CSV format: {}",
            i + 1,
            line
        );
    }
}
