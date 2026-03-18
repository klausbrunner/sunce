mod common;
use common::*;
use predicates::prelude::*;
use std::collections::{HashMap, HashSet};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

fn output_text(args: &[&str], envs: &[(&str, &str)]) -> String {
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
    String::from_utf8(output).unwrap()
}

fn csv_rows(args: &[&str], envs: &[(&str, &str)]) -> Vec<HashMap<String, String>> {
    parse_csv_output_maps(&output_text(args, envs))
}

fn csv_row(args: &[&str], envs: &[(&str, &str)]) -> HashMap<String, String> {
    parse_csv_single_record_map(&output_text(args, envs))
}

#[test]
fn test_basic_position_calculation() {
    position_test().assert_success_contains_all(&["dateTime", "azimuth", "zenith"]);
}

#[test]
fn test_output_formats() {
    position_test().assert_success_contains("azimuth");

    let csv = output_text(
        &[
            "--format=CSV",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ],
        &[],
    );
    let (headers, rows) = parse_csv_output(&csv);
    assert_eq!(headers, fields(&["dateTime", "azimuth", "zenith"]));
    assert_eq!(rows.len(), 1);

    let json = parse_json_output(&output_text(
        &[
            "--format=JSON",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ],
        &[],
    ));
    assert!(json.get("dateTime").is_some());
    assert!(json.get("azimuth").is_some());
    assert!(json.get("zenith").is_some());

    #[cfg(feature = "parquet")]
    {
        let output = position_test_with_format("PARQUET").get_output();
        assert!(output.status.success());
        assert!(!output.stdout.is_empty());
    }

    #[cfg(not(feature = "parquet"))]
    {
        let output = position_test_with_format("PARQUET").get_output();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("PARQUET format not available in this build")
        );
    }
}

#[test]
fn test_position_command_variants() {
    for algorithm in ["SPA", "GRENA3"] {
        position_test()
            .arg(format!("--algorithm={algorithm}"))
            .assert_success();
    }

    let default_headers = parse_csv_output(&output_text(
        &[
            "--format=CSV",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ],
        &[],
    ))
    .0;
    assert!(default_headers.contains(&"zenith".to_string()));

    let elevation_headers = parse_csv_output(&output_text(
        &[
            "--format=CSV",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "--elevation-angle",
        ],
        &[],
    ))
    .0;
    assert!(elevation_headers.contains(&"elevation-angle".to_string()));

    let with_refraction = output_text(&["52.0", "13.4", "2024-01-01T12:00:00", "position"], &[]);
    let without_refraction = output_text(
        &[
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "--no-refraction",
        ],
        &[],
    );
    assert_ne!(with_refraction, without_refraction);
}

#[test]
fn test_coordinate_ranges() {
    let rows = csv_rows(
        &[
            "--format=CSV",
            "52:53:1",
            "13:14:1",
            "2024-01-01T12:00:00",
            "position",
        ],
        &[],
    );
    let combos = rows
        .iter()
        .map(|row| (row["latitude"].clone(), row["longitude"].clone()))
        .collect::<HashSet<_>>();
    assert_eq!(rows.len(), 4);
    assert_eq!(
        combos,
        HashSet::from([
            ("52.00000".to_string(), "13.00000".to_string()),
            ("52.00000".to_string(), "14.00000".to_string()),
            ("53.00000".to_string(), "13.00000".to_string()),
            ("53.00000".to_string(), "14.00000".to_string()),
        ])
    );
}

#[test]
fn test_time_series_generation_and_partial_dates() {
    let datetimes = csv_rows(
        &[
            "--format=CSV",
            "52.0",
            "13.4",
            "2024-01-01",
            "position",
            "--step=6h",
        ],
        &[],
    )
    .into_iter()
    .map(|row| row["dateTime"].clone())
    .collect::<HashSet<_>>();
    for expected in [
        "2024-01-01T00:00:00",
        "2024-01-01T06:00:00",
        "2024-01-01T12:00:00",
        "2024-01-01T18:00:00",
    ] {
        assert!(datetimes.iter().any(|ts| ts.starts_with(expected)));
    }

    for (date, step, expected) in [
        (
            "2024",
            "24h",
            ["2024-01-01T00:00:00", "2024-12-31T00:00:00"],
        ),
        (
            "2024-06",
            "24h",
            ["2024-06-01T00:00:00", "2024-06-30T00:00:00"],
        ),
    ] {
        let rows = csv_rows(
            &[
                "--format=CSV",
                "52.0",
                "13.4",
                date,
                "position",
                &format!("--step={step}"),
            ],
            &[],
        );
        let datetimes = rows
            .into_iter()
            .map(|row| row["dateTime"].clone())
            .collect::<Vec<_>>();
        assert!(datetimes.iter().any(|ts| ts.starts_with(expected[0])));
        assert!(datetimes.iter().any(|ts| ts.starts_with(expected[1])));
    }
}

#[test]
fn test_show_inputs_and_environmental_fields() {
    let auto_headers = parse_csv_output(&output_text(
        &[
            "--format=CSV",
            "52:53:1",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ],
        &[],
    ))
    .0;
    assert_eq!(
        auto_headers,
        fields(&[
            "latitude",
            "longitude",
            "elevation",
            "pressure",
            "temperature",
            "dateTime",
            "deltaT",
            "azimuth",
            "zenith",
        ])
    );

    let hidden_headers = parse_csv_output(&output_text(
        &[
            "--format=CSV",
            "--no-show-inputs",
            "52:53:1",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ],
        &[],
    ))
    .0;
    assert_eq!(hidden_headers, fields(&["dateTime", "azimuth", "zenith"]));

    let env_row = csv_row(
        &[
            "--format=CSV",
            "--show-inputs",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "--elevation=1000",
            "--pressure=900",
            "--temperature=25",
        ],
        &[],
    );
    assert_eq!(
        env_row.get("elevation").map(String::as_str),
        Some("1000.000")
    );
    assert_eq!(env_row.get("pressure").map(String::as_str), Some("900.000"));
    assert_eq!(
        env_row.get("temperature").map(String::as_str),
        Some("25.000")
    );
}

#[test]
fn test_timezone_and_datetime_parsing() {
    for (tz, expected_suffix) in [("+02:00", "+02:00"), ("UTC", "+00:00")] {
        let row = csv_row(
            &[
                &format!("--timezone={tz}"),
                "--format=CSV",
                "--show-inputs",
                "52.0",
                "13.4",
                "2024-01-01T12:00:00",
                "position",
            ],
            &[],
        );
        assert!(row["dateTime"].ends_with(expected_suffix));
    }

    for input in [
        "2024-01-01T12:00:00",
        "2024-01-01 12:00:00",
        "2024-01-01T12:00:00Z",
        "2024-01-01T12:00:00+01:00",
    ] {
        custom_position("52.0", "13.4", input).assert_success();
    }

    let json = parse_json_output(&output_text(
        &[
            "--format=JSON",
            "--no-show-inputs",
            "0",
            "0",
            "2024-01-01 12:00",
            "position",
        ],
        &[("TZ", "UTC")],
    ));
    assert_eq!(
        json.get("dateTime").and_then(serde_json::Value::as_str),
        Some("2024-01-01T12:00:00+00:00")
    );
}

#[test]
fn test_time_steps_and_edge_locations() {
    for (date, step) in [
        ("2024-01-01", "30s"),
        ("2024-01-01", "15m"),
        ("2024-01-01", "2h"),
        ("2024-01", "7d"),
    ] {
        time_series_test(date, step).assert_success();
    }

    for (lat, lon, datetime) in [
        ("90.0", "0.0", "2024-06-21T12:00:00"),
        ("-90.0", "0.0", "2024-12-21T12:00:00"),
        ("0.0", "180.0", "2024-01-01T12:00:00"),
    ] {
        custom_position(lat, lon, datetime).assert_success();
    }
}

#[test]
fn test_combined_range_and_now_behavior() {
    let rows = csv_rows(
        &[
            "--format=CSV",
            "52:53:1",
            "13:14:1",
            "2024-01-01",
            "position",
            "--step=12h",
        ],
        &[],
    );
    assert_eq!(rows.len(), 8);

    custom_position("52.0", "13.4", "now").assert_success();

    let rows = csv_rows(
        &[
            "--format=CSV",
            "--show-inputs",
            "52:53:1",
            "13.4",
            "now",
            "position",
        ],
        &[],
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["dateTime"], rows[1]["dateTime"]);
}

#[test]
fn test_csv_headers_and_delta_t() {
    let with_headers = parse_csv_output(&output_text(
        &[
            "--format=CSV",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ],
        &[],
    ))
    .0;
    assert_eq!(with_headers, fields(&["dateTime", "azimuth", "zenith"]));

    let rows = parse_csv_no_headers_output(&output_text(
        &[
            "--format=CSV",
            "--no-headers",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ],
        &[],
    ));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 3);

    let explicit = csv_row(
        &[
            "--format=CSV",
            "--show-inputs",
            "--deltat=69.2",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ],
        &[],
    );
    assert_eq!(explicit.get("deltaT").map(String::as_str), Some("69.200"));

    position_with_deltat_estimation().assert_success();
}

#[test]
fn test_validation_and_error_handling() {
    custom_position("91.0", "13.4", "2024-01-01T12:00:00")
        .assert_failure()
        .stderr(predicate::str::contains("Latitude must be between"));
    custom_position("52.0", "181.0", "2024-01-01T12:00:00")
        .assert_failure()
        .stderr(predicate::str::contains("Longitude must be between"));

    for args in [
        vec!["52.0"],
        vec![
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "--step=invalid",
        ],
        vec![
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "--algorithm=INVALID",
        ],
    ] {
        SunceTest::new().args(args).assert_failure();
    }
}

#[test]
fn test_unix_timestamps() {
    let basic = csv_row(
        &[
            "--format=CSV",
            "--show-inputs",
            "52.0",
            "13.4",
            "1577836800",
            "position",
        ],
        &[],
    );
    assert!(basic["dateTime"].starts_with("2020-01-01T00:00:00"));

    for tz in ["+01:00", "Europe/Berlin"] {
        let row = csv_row(
            &[
                &format!("--timezone={tz}"),
                "--format=CSV",
                "--show-inputs",
                "52.0",
                "13.4",
                "1577836800",
                "position",
            ],
            &[],
        );
        assert_eq!(row["dateTime"], "2020-01-01T01:00:00+01:00");
    }

    for (input, expected_prefix) in [
        ("946684800", "2000-01-01"),
        ("1577836800", "2020-01-01"),
        ("10000", "1970-01-01T02:46:40"),
        ("-10000", "1969-12-31"),
    ] {
        let row = csv_row(
            &[
                "--format=CSV",
                "--show-inputs",
                "52.0",
                "13.4",
                input,
                "position",
            ],
            &[],
        );
        assert!(row["dateTime"].starts_with(expected_prefix));
    }

    let year_output = output_text(
        &[
            "--format=CSV",
            "--show-inputs",
            "52.0",
            "13.4",
            "9999",
            "position",
        ],
        &[],
    );
    assert!(year_output.contains("9999-01-01"));
}

#[test]
fn test_unix_timestamp_in_files_and_range_precision() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("timestamps.txt");
    write_text_file(&file, "52.0 13.4 1577836800\n");
    let rows = csv_rows(
        &["--format=CSV", &format!("@{}", file.display()), "position"],
        &[],
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["latitude"], "52.00000");
    assert_eq!(rows[0]["longitude"], "13.40000");
    assert!(rows[0]["dateTime"].starts_with("2020-01-01"));

    for (range, expected) in [
        ("50.0:50.2:0.1", vec!["50.00000", "50.10000", "50.20000"]),
        (
            "0.0:0.9:0.3",
            vec!["0.00000", "0.30000", "0.60000", "0.90000"],
        ),
    ] {
        let rows = csv_rows(
            &[
                "--format=CSV",
                range,
                "10.0",
                "2024-01-01T12:00:00",
                "position",
            ],
            &[],
        );
        let values = rows
            .into_iter()
            .map(|row| row["latitude"].clone())
            .collect::<Vec<_>>();
        assert_eq!(values, expected);
    }
}

#[test]
fn test_watch_mode() {
    let mut child = Command::new(assert_cmd::cargo::cargo_bin!("sunce"))
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
    let lines = String::from_utf8(output.stdout).unwrap();
    let rows = lines.lines().collect::<Vec<_>>();

    assert!(rows.len() >= 3, "expected header plus at least 2 data rows");
    assert_eq!(rows[0], "dateTime,azimuth,zenith");
    assert!(rows.iter().skip(1).all(|line| line.contains(',')));
}
