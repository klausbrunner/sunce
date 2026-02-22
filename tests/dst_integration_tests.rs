mod common;
use common::{csv_row_map, parse_csv_output, parse_json_output, sunce_command};
use serde_json::Value;

fn csv_single_record(output: std::process::Output) -> std::collections::HashMap<String, String> {
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let (headers, rows) = parse_csv_output(&stdout);
    assert_eq!(rows.len(), 1, "expected one CSV record");
    csv_row_map(&headers, &rows[0])
}

fn csv_datetimes(output: std::process::Output) -> Vec<String> {
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let (headers, rows) = parse_csv_output(&stdout);
    rows.iter()
        .map(|row| {
            let record = csv_row_map(&headers, row);
            record["dateTime"].clone()
        })
        .collect()
}

fn no_header_csv_datetimes(stdout: &str) -> Vec<String> {
    stdout
        .lines()
        .map(|line| {
            line.split(',')
                .find(|field| {
                    field.contains('T')
                        && (field.contains("+00:00")
                            || field.contains("+01:00")
                            || field.contains("+02:00")
                            || field.contains("-04:00")
                            || field.contains("-05:00"))
                })
                .unwrap_or_else(|| panic!("datetime field not found in line: {}", line))
                .to_string()
        })
        .collect::<Vec<_>>()
}

#[test]
fn test_dst_spring_forward_single_datetime() {
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=+01:00",
        "--show-inputs",
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-03-31T02:00:00",
        "position",
    ]);

    let record = csv_single_record(cmd.output().unwrap());
    assert_eq!(
        record.get("dateTime").map(String::as_str),
        Some("2024-03-31T02:00:00+01:00")
    );
    assert_eq!(record.get("azimuth").map(String::as_str), Some("31.6478"));
}

#[test]
fn test_dst_spring_forward_time_series() {
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=+01:00",
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-03-31",
        "position",
        "--step=1h",
    ]);

    let datetimes = csv_datetimes(cmd.output().unwrap());
    assert!(datetimes.contains(&"2024-03-31T00:00:00+01:00".to_string()));
    assert!(datetimes.contains(&"2024-03-31T01:00:00+01:00".to_string()));
    assert!(datetimes.contains(&"2024-03-31T02:00:00+01:00".to_string()));
    assert!(datetimes.contains(&"2024-03-31T03:00:00+01:00".to_string()));
}

#[test]
fn test_dst_fall_back_single_datetime() {
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=+01:00",
        "--show-inputs",
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-10-27T02:00:00",
        "position",
    ]);

    let record = csv_single_record(cmd.output().unwrap());
    assert_eq!(
        record.get("dateTime").map(String::as_str),
        Some("2024-10-27T02:00:00+01:00")
    );
}

#[test]
fn test_dst_fall_back_time_series() {
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=+01:00",
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-10-27",
        "position",
        "--step=1h",
    ]);

    let datetimes = csv_datetimes(cmd.output().unwrap());
    assert!(datetimes.contains(&"2024-10-27T01:00:00+01:00".to_string()));
    assert!(datetimes.contains(&"2024-10-27T02:00:00+01:00".to_string()));
    assert!(datetimes.contains(&"2024-10-27T03:00:00+01:00".to_string()));
}

#[test]
fn test_dst_normal_summer_time() {
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=+02:00",
        "--show-inputs",
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-07-15T12:00:00",
        "position",
    ]);

    let record = csv_single_record(cmd.output().unwrap());
    assert_eq!(
        record.get("dateTime").map(String::as_str),
        Some("2024-07-15T12:00:00+02:00")
    );
}

#[test]
fn test_dst_normal_winter_time() {
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=+01:00",
        "--show-inputs",
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-01-15T12:00:00",
        "position",
    ]);

    let record = csv_single_record(cmd.output().unwrap());
    assert_eq!(
        record.get("dateTime").map(String::as_str),
        Some("2024-01-15T12:00:00+01:00")
    );
}

#[test]
fn test_dst_different_timezone_us_eastern() {
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=-05:00",
        "--show-inputs",
        "--format=CSV",
        "40.7",
        "-74.0",
        "2024-03-10T02:00:00",
        "position",
    ]);

    let record = csv_single_record(cmd.output().unwrap());
    assert_eq!(
        record.get("dateTime").map(String::as_str),
        Some("2024-03-10T02:00:00-05:00")
    );
}

#[test]
fn test_dst_timezone_override() {
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=+02:00",
        "--show-inputs",
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-03-31T02:00:00",
        "position",
    ]);

    let record = csv_single_record(cmd.output().unwrap());
    assert_eq!(
        record.get("dateTime").map(String::as_str),
        Some("2024-03-31T02:00:00+02:00")
    );
}

#[test]
fn test_named_timezone_override_uses_dst_offset_summer() {
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=America/New_York",
        "--format=json",
        "--no-show-inputs",
        "0",
        "0",
        "2025-06-21T12:00:00Z",
        "position",
    ]);

    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let json = parse_json_output(&stdout);
    assert_eq!(
        json.get("dateTime").and_then(Value::as_str),
        Some("2025-06-21T08:00:00-04:00")
    );
}

#[test]
fn test_named_timezone_override_uses_dst_offset_winter() {
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=America/New_York",
        "--format=json",
        "--no-show-inputs",
        "0",
        "0",
        "2025-01-15T12:00:00Z",
        "position",
    ]);

    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let json = parse_json_output(&stdout);
    assert_eq!(
        json.get("dateTime").and_then(Value::as_str),
        Some("2025-01-15T07:00:00-05:00")
    );
}

#[test]
fn test_system_timezone_detection_without_tz_env_summer() {
    let mut cmd = sunce_command();
    cmd.env_remove("TZ")
        .env("SUNCE_SYSTEM_TIMEZONE", "Europe/Berlin")
        .args([
            "--format=json",
            "--no-show-inputs",
            "0",
            "0",
            "2024-07-01T12:00:00",
            "position",
        ]);

    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let json = parse_json_output(&stdout);
    assert_eq!(
        json.get("dateTime").and_then(Value::as_str),
        Some("2024-07-01T12:00:00+02:00")
    );
}

#[test]
fn test_system_timezone_detection_without_tz_env_winter() {
    let mut cmd = sunce_command();
    cmd.env_remove("TZ")
        .env("SUNCE_SYSTEM_TIMEZONE", "Europe/Berlin")
        .args([
            "--format=json",
            "--no-show-inputs",
            "0",
            "0",
            "2024-01-10T12:00:00",
            "position",
        ]);

    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let json = parse_json_output(&stdout);
    assert_eq!(
        json.get("dateTime").and_then(Value::as_str),
        Some("2024-01-10T12:00:00+01:00")
    );
}

#[test]
fn test_dst_partial_date_time_series() {
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=+01:00",
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-03",
        "position",
        "--step=24h",
    ]);

    let datetimes = csv_datetimes(cmd.output().unwrap());
    assert!(datetimes.contains(&"2024-03-30T00:00:00+01:00".to_string()));
    assert!(datetimes.contains(&"2024-03-31T00:00:00+01:00".to_string()));
    assert!(datetimes.iter().all(|ts| ts.ends_with("+01:00")));
}

#[test]
fn test_dst_year_time_series() {
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=+01:00",
        "--format=CSV",
        "52.0",
        "13.4",
        "2024",
        "position",
        "--step=24h",
    ]);

    let datetimes = csv_datetimes(cmd.output().unwrap());
    assert!(datetimes.contains(&"2024-03-30T00:00:00+01:00".to_string()));
    assert!(datetimes.contains(&"2024-04-01T00:00:00+01:00".to_string()));
    assert!(datetimes.contains(&"2024-10-26T00:00:00+01:00".to_string()));
    assert!(datetimes.contains(&"2024-10-28T00:00:00+01:00".to_string()));
}

#[test]
fn test_dst_edge_case_31st_march_exact_time() {
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=+01:00",
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-03-31",
        "position",
        "--step=30m",
    ]);

    let datetimes = csv_datetimes(cmd.output().unwrap());
    assert!(datetimes.contains(&"2024-03-31T01:00:00+01:00".to_string()));
    assert!(datetimes.contains(&"2024-03-31T02:00:00+01:00".to_string()));
    assert!(datetimes.contains(&"2024-03-31T02:30:00+01:00".to_string()));
    assert!(datetimes.contains(&"2024-03-31T03:00:00+01:00".to_string()));
    assert!(datetimes.iter().all(|ts| ts.ends_with("+01:00")));
}

#[test]
fn test_dst_comparison_with_utc() {
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=UTC",
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-03-31",
        "position",
        "--step=1h",
    ]);

    let datetimes = csv_datetimes(cmd.output().unwrap());
    assert!(datetimes.contains(&"2024-03-31T01:00:00+00:00".to_string()));
    assert!(datetimes.contains(&"2024-03-31T02:00:00+00:00".to_string()));
    assert!(datetimes.contains(&"2024-03-31T03:00:00+00:00".to_string()));
}

#[test]
fn test_dst_named_timezone_spring_forward() {
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=Europe/Berlin",
        "--format=CSV",
        "--no-headers",
        "52.0",
        "13.4",
        "2024-03-31",
        "position",
        "--step=1h",
    ]);

    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let datetimes = no_header_csv_datetimes(&stdout);

    assert!(datetimes.contains(&"2024-03-31T00:00:00+01:00".to_string()));
    assert!(datetimes.contains(&"2024-03-31T01:00:00+01:00".to_string()));
    assert!(
        !datetimes
            .iter()
            .any(|ts| ts.starts_with("2024-03-31T02:00:00"))
    );
    assert!(datetimes.contains(&"2024-03-31T03:00:00+02:00".to_string()));
    assert!(datetimes.contains(&"2024-03-31T04:00:00+02:00".to_string()));
}

#[test]
fn test_dst_named_timezone_fall_back() {
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=Europe/Berlin",
        "--format=CSV",
        "--no-headers",
        "52.0",
        "13.4",
        "2024-10-27",
        "position",
        "--step=1h",
    ]);

    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let datetimes = no_header_csv_datetimes(&stdout);

    assert!(datetimes.contains(&"2024-10-27T01:00:00+02:00".to_string()));
    assert!(datetimes.contains(&"2024-10-27T02:00:00+02:00".to_string()));
    assert!(datetimes.contains(&"2024-10-27T02:00:00+01:00".to_string()));
    assert!(datetimes.contains(&"2024-10-27T03:00:00+01:00".to_string()));
    assert!(datetimes.contains(&"2024-10-27T04:00:00+01:00".to_string()));
}

#[test]
fn test_dst_named_timezone_us_eastern() {
    let mut cmd = sunce_command();
    cmd.args([
        "--timezone=America/New_York",
        "--format=CSV",
        "--no-headers",
        "40.7",
        "-74.0",
        "2024-03-10",
        "position",
        "--step=1h",
    ]);

    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let datetimes = no_header_csv_datetimes(&stdout);

    assert!(datetimes.contains(&"2024-03-10T01:00:00-05:00".to_string()));
    assert!(
        !datetimes
            .iter()
            .any(|ts| ts.starts_with("2024-03-10T02:00:00"))
    );
    assert!(datetimes.contains(&"2024-03-10T03:00:00-04:00".to_string()));
}

#[test]
fn test_system_timezone_detection() {
    let mut cmd = sunce_command();
    cmd.args([
        "--show-inputs",
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-01-15T12:00:00",
        "position",
    ]);

    let record = csv_single_record(cmd.output().unwrap());
    let datetime = record.get("dateTime").unwrap();
    assert!(datetime.starts_with("2024-01-15T12:00:00"));
    assert!(datetime.contains('+') || datetime.contains('-'));
}

#[test]
fn test_now_respects_tz_env() {
    let mut cmd = sunce_command();
    cmd.env("TZ", "America/New_York");
    cmd.args(["--format=CSV", "40.7", "-74.0", "now", "position"]);

    let datetimes = csv_datetimes(cmd.output().unwrap());
    assert!(!datetimes.is_empty());
    let datetime = &datetimes[0];
    assert!(datetime.ends_with("-05:00") || datetime.ends_with("-04:00"));
    assert!(!datetime.ends_with("+00:00"));
}

#[test]
fn test_now_respects_tz_env_fixed_offset() {
    let mut cmd = sunce_command();
    cmd.env("TZ", "+05:30");
    cmd.args(["--format=CSV", "28.6", "77.2", "now", "position"]);

    let datetimes = csv_datetimes(cmd.output().unwrap());
    assert!(!datetimes.is_empty());
    let datetime = &datetimes[0];
    assert!(datetime.ends_with("+05:30"));
    assert!(!datetime.ends_with("+00:00"));
}

#[test]
fn test_now_table_format_shows_timezone() {
    let mut cmd = sunce_command();
    cmd.env("TZ", "Europe/Paris");
    cmd.args(["48.8", "2.3", "now", "position"]);

    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let output_str = String::from_utf8(output.stdout).unwrap();

    let has_paris_tz = output_str.contains("+01:00") || output_str.contains("+02:00");
    assert!(has_paris_tz);
    assert!(output_str.contains("dateTime"));
}
