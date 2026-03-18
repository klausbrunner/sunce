mod common;
use chrono::{DateTime, FixedOffset};
use common::{
    parse_csv_no_headers_output, parse_csv_output_maps, parse_csv_single_record_map,
    parse_json_output, sunce_command,
};
use serde_json::Value;
use std::collections::HashMap;

type SeriesCase<'a> = (Vec<&'a str>, Vec<&'a str>, Option<&'a str>, Option<&'a str>);
type NamedZoneCase<'a> = (Vec<&'a str>, Vec<&'a str>, Option<&'a str>);

fn output_text(args: &[&str], envs: &[(&str, Option<&str>)]) -> String {
    let mut cmd = sunce_command();
    for (key, value) in envs {
        match value {
            Some(value) => {
                cmd.env(key, value);
            }
            None => {
                cmd.env_remove(key);
            }
        }
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

fn csv_row(args: &[&str], envs: &[(&str, Option<&str>)]) -> HashMap<String, String> {
    parse_csv_single_record_map(&output_text(args, envs))
}

fn csv_datetimes(args: &[&str], envs: &[(&str, Option<&str>)]) -> Vec<String> {
    parse_csv_output_maps(&output_text(args, envs))
        .into_iter()
        .map(|row| row["dateTime"].clone())
        .collect()
}

fn no_header_datetimes(args: &[&str], envs: &[(&str, Option<&str>)]) -> Vec<String> {
    parse_csv_no_headers_output(&output_text(args, envs))
        .into_iter()
        .map(|row| {
            row.into_iter()
                .find(|field| DateTime::<FixedOffset>::parse_from_rfc3339(field).is_ok())
                .expect("datetime field not found")
        })
        .collect()
}

#[test]
fn test_fixed_offset_single_datetimes() {
    for (args, expected) in [
        (
            vec![
                "--timezone=+01:00",
                "--show-inputs",
                "--format=CSV",
                "52.0",
                "13.4",
                "2024-03-31T02:00:00",
                "position",
            ],
            "2024-03-31T02:00:00+01:00",
        ),
        (
            vec![
                "--timezone=+01:00",
                "--show-inputs",
                "--format=CSV",
                "52.0",
                "13.4",
                "2024-10-27T02:00:00",
                "position",
            ],
            "2024-10-27T02:00:00+01:00",
        ),
        (
            vec![
                "--timezone=+02:00",
                "--show-inputs",
                "--format=CSV",
                "52.0",
                "13.4",
                "2024-07-15T12:00:00",
                "position",
            ],
            "2024-07-15T12:00:00+02:00",
        ),
        (
            vec![
                "--timezone=+01:00",
                "--show-inputs",
                "--format=CSV",
                "52.0",
                "13.4",
                "2024-01-15T12:00:00",
                "position",
            ],
            "2024-01-15T12:00:00+01:00",
        ),
        (
            vec![
                "--timezone=-05:00",
                "--show-inputs",
                "--format=CSV",
                "40.7",
                "-74.0",
                "2024-03-10T02:00:00",
                "position",
            ],
            "2024-03-10T02:00:00-05:00",
        ),
        (
            vec![
                "--timezone=+02:00",
                "--show-inputs",
                "--format=CSV",
                "52.0",
                "13.4",
                "2024-03-31T02:00:00",
                "position",
            ],
            "2024-03-31T02:00:00+02:00",
        ),
    ] {
        let row = csv_row(&args, &[]);
        assert_eq!(row.get("dateTime").map(String::as_str), Some(expected));
    }

    let spring = csv_row(
        &[
            "--timezone=+01:00",
            "--show-inputs",
            "--format=CSV",
            "52.0",
            "13.4",
            "2024-03-31T02:00:00",
            "position",
        ],
        &[],
    );
    assert_eq!(spring.get("azimuth").map(String::as_str), Some("31.6478"));
}

#[test]
fn test_fixed_offset_time_series_are_stable() {
    let cases: [SeriesCase<'_>; 6] = [
        (
            vec![
                "--timezone=+01:00",
                "--format=CSV",
                "52.0",
                "13.4",
                "2024-03-31",
                "position",
                "--step=1h",
            ],
            vec![
                "2024-03-31T00:00:00+01:00",
                "2024-03-31T01:00:00+01:00",
                "2024-03-31T02:00:00+01:00",
                "2024-03-31T03:00:00+01:00",
            ],
            None,
            None,
        ),
        (
            vec![
                "--timezone=+01:00",
                "--format=CSV",
                "52.0",
                "13.4",
                "2024-10-27",
                "position",
                "--step=1h",
            ],
            vec![
                "2024-10-27T01:00:00+01:00",
                "2024-10-27T02:00:00+01:00",
                "2024-10-27T03:00:00+01:00",
            ],
            None,
            None,
        ),
        (
            vec![
                "--timezone=+01:00",
                "--format=CSV",
                "52.0",
                "13.4",
                "2024-03",
                "position",
                "--step=24h",
            ],
            vec!["2024-03-30T00:00:00+01:00", "2024-03-31T00:00:00+01:00"],
            None,
            Some("+01:00"),
        ),
        (
            vec![
                "--timezone=+01:00",
                "--format=CSV",
                "52.0",
                "13.4",
                "2024",
                "position",
                "--step=24h",
            ],
            vec![
                "2024-03-30T00:00:00+01:00",
                "2024-04-01T00:00:00+01:00",
                "2024-10-26T00:00:00+01:00",
                "2024-10-28T00:00:00+01:00",
            ],
            None,
            None,
        ),
        (
            vec![
                "--timezone=+01:00",
                "--format=CSV",
                "52.0",
                "13.4",
                "2024-03-31",
                "position",
                "--step=30m",
            ],
            vec![
                "2024-03-31T01:00:00+01:00",
                "2024-03-31T02:00:00+01:00",
                "2024-03-31T02:30:00+01:00",
                "2024-03-31T03:00:00+01:00",
            ],
            None,
            Some("+01:00"),
        ),
        (
            vec![
                "--timezone=UTC",
                "--format=CSV",
                "52.0",
                "13.4",
                "2024-03-31",
                "position",
                "--step=1h",
            ],
            vec![
                "2024-03-31T01:00:00+00:00",
                "2024-03-31T02:00:00+00:00",
                "2024-03-31T03:00:00+00:00",
            ],
            None,
            None,
        ),
    ];
    for (args, expected, absent_prefix, required_suffix) in cases {
        let datetimes = csv_datetimes(&args, &[]);
        for datetime in expected {
            assert!(datetimes.contains(&datetime.to_string()));
        }
        if let Some(prefix) = absent_prefix {
            assert!(!datetimes.iter().any(|ts| ts.starts_with(prefix)));
        }
        if let Some(suffix) = required_suffix {
            assert!(datetimes.iter().all(|ts| ts.ends_with(suffix)));
        }
    }
}

#[test]
fn test_named_timezones_apply_real_dst_rules() {
    let cases: [NamedZoneCase<'_>; 3] = [
        (
            vec![
                "--timezone=Europe/Berlin",
                "--format=CSV",
                "--no-headers",
                "52.0",
                "13.4",
                "2024-03-31",
                "position",
                "--step=1h",
            ],
            vec![
                "2024-03-31T00:00:00+01:00",
                "2024-03-31T01:00:00+01:00",
                "2024-03-31T03:00:00+02:00",
                "2024-03-31T04:00:00+02:00",
            ],
            Some("2024-03-31T02:00:00"),
        ),
        (
            vec![
                "--timezone=Europe/Berlin",
                "--format=CSV",
                "--no-headers",
                "52.0",
                "13.4",
                "2024-10-27",
                "position",
                "--step=1h",
            ],
            vec![
                "2024-10-27T01:00:00+02:00",
                "2024-10-27T02:00:00+02:00",
                "2024-10-27T02:00:00+01:00",
                "2024-10-27T03:00:00+01:00",
                "2024-10-27T04:00:00+01:00",
            ],
            None,
        ),
        (
            vec![
                "--timezone=America/New_York",
                "--format=CSV",
                "--no-headers",
                "40.7",
                "-74.0",
                "2024-03-10",
                "position",
                "--step=1h",
            ],
            vec!["2024-03-10T01:00:00-05:00", "2024-03-10T03:00:00-04:00"],
            Some("2024-03-10T02:00:00"),
        ),
    ];
    for (args, expected, missing_prefix) in cases {
        let datetimes = no_header_datetimes(&args, &[]);
        for datetime in expected {
            assert!(datetimes.contains(&datetime.to_string()));
        }
        if let Some(prefix) = missing_prefix {
            assert!(!datetimes.iter().any(|ts| ts.starts_with(prefix)));
        }
    }
}

#[test]
fn test_named_timezone_override_offsets() {
    for (datetime, expected) in [
        ("2025-06-21T12:00:00Z", "2025-06-21T08:00:00-04:00"),
        ("2025-01-15T12:00:00Z", "2025-01-15T07:00:00-05:00"),
    ] {
        let json = parse_json_output(&output_text(
            &[
                "--timezone=America/New_York",
                "--format=json",
                "--no-show-inputs",
                "0",
                "0",
                datetime,
                "position",
            ],
            &[],
        ));
        assert_eq!(json.get("dateTime").and_then(Value::as_str), Some(expected));
    }
}

#[test]
fn test_system_timezone_detection_paths() {
    for (datetime, expected) in [
        ("2024-07-01T12:00:00", "2024-07-01T12:00:00+02:00"),
        ("2024-01-10T12:00:00", "2024-01-10T12:00:00+01:00"),
    ] {
        let json = parse_json_output(&output_text(
            &[
                "--format=json",
                "--no-show-inputs",
                "0",
                "0",
                datetime,
                "position",
            ],
            &[
                ("TZ", None),
                ("SUNCE_SYSTEM_TIMEZONE", Some("Europe/Berlin")),
            ],
        ));
        assert_eq!(json.get("dateTime").and_then(Value::as_str), Some(expected));
    }

    let row = csv_row(
        &[
            "--show-inputs",
            "--format=CSV",
            "52.0",
            "13.4",
            "2024-01-15T12:00:00",
            "position",
        ],
        &[],
    );
    let datetime = row.get("dateTime").unwrap();
    assert!(datetime.starts_with("2024-01-15T12:00:00"));
    assert!(datetime.contains('+') || datetime.contains('-'));
}

#[test]
fn test_now_respects_timezone_sources() {
    for (envs, args, suffixes) in [
        (
            vec![("TZ", Some("America/New_York"))],
            vec!["--format=CSV", "40.7", "-74.0", "now", "position"],
            vec!["-05:00", "-04:00"],
        ),
        (
            vec![("TZ", Some("+05:30"))],
            vec!["--format=CSV", "28.6", "77.2", "now", "position"],
            vec!["+05:30"],
        ),
    ] {
        let datetimes = csv_datetimes(&args, &envs);
        assert!(!datetimes.is_empty());
        let datetime = &datetimes[0];
        assert!(suffixes.iter().any(|suffix| datetime.ends_with(suffix)));
        assert!(!datetime.ends_with("+00:00"));
    }

    let output = output_text(
        &["48.8", "2.3", "now", "position"],
        &[("TZ", Some("Europe/Paris"))],
    );
    assert!(output.contains("dateTime"));
    assert!(output.contains("+01:00") || output.contains("+02:00"));
}
