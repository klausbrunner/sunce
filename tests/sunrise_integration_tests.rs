mod common;
use common::{
    assert_time_close, fields, parse_csv_output, parse_csv_output_maps,
    parse_csv_single_record_map, parse_json_output, sunce_command,
};
use serde_json::Value;
use std::collections::HashSet;

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

fn csv_rows(
    args: &[&str],
    envs: &[(&str, &str)],
) -> Vec<std::collections::HashMap<String, String>> {
    parse_csv_output_maps(&output_text(args, envs))
}

fn csv_row(args: &[&str], envs: &[(&str, &str)]) -> std::collections::HashMap<String, String> {
    parse_csv_single_record_map(&output_text(args, envs))
}

#[test]
fn test_sunrise_output_formats() {
    let human = output_text(&["52.0", "13.4", "2024-06-21", "sunrise"], &[]);
    assert!(human.contains("sunrise"));
    assert!(human.contains("sunset"));

    let csv = output_text(
        &["--format=CSV", "52.0", "13.4", "2024-06-21", "sunrise"],
        &[],
    );
    let (headers, rows) = parse_csv_output(&csv);
    assert_eq!(
        headers,
        fields(&["dateTime", "type", "sunrise", "transit", "sunset"])
    );
    assert_eq!(rows.len(), 1);

    let json = parse_json_output(&output_text(
        &["--format=JSON", "52.0", "13.4", "2024-06-21", "sunrise"],
        &[],
    ));
    assert!(json.get("type").is_some());
    assert!(json.get("sunrise").is_some());
    assert!(json.get("sunset").is_some());
}

#[test]
fn test_sunrise_polar_edge_cases() {
    for (date, expected) in [("2024-06-21", "ALL_DAY"), ("2024-12-21", "ALL_NIGHT")] {
        let json = parse_json_output(&output_text(
            &["--format=JSON", "80.0", "0.0", date, "sunrise"],
            &[],
        ));
        assert_eq!(json.get("type").and_then(Value::as_str), Some(expected));
    }
}

#[test]
fn test_sunrise_time_series_and_coordinate_ranges() {
    let month_rows = csv_rows(
        &["--format=CSV", "52.0", "13.4", "2024-06", "sunrise"],
        &[("TZ", "Europe/Berlin")],
    );
    let datetimes = month_rows
        .iter()
        .map(|row| row["dateTime"].clone())
        .collect::<HashSet<_>>();
    for expected in [
        "2024-06-01T00:00:00+02:00",
        "2024-06-15T00:00:00+02:00",
        "2024-06-30T00:00:00+02:00",
    ] {
        assert!(datetimes.contains(expected));
    }

    let range_rows = csv_rows(
        &[
            "--format=CSV",
            "52:53:1",
            "13:14:1",
            "2024-06-21",
            "sunrise",
        ],
        &[("TZ", "Europe/Berlin")],
    );
    assert_eq!(range_rows.len(), 4);
}

#[test]
fn test_sunrise_timezone_handling() {
    let json = parse_json_output(&output_text(
        &[
            "--format=JSON",
            "--timezone=+02:00",
            "52.0",
            "13.4",
            "2024-06-21",
            "sunrise",
        ],
        &[],
    ));
    for field in ["sunrise", "transit", "sunset"] {
        assert!(json[field].as_str().unwrap().ends_with("+02:00"));
    }

    for (lat, lon, date, tz) in [
        ("52.0", "13.4", "2024-06-21", "+02:00"),
        ("40.7", "-74.0", "2024-12-21", "-05:00"),
    ] {
        let row = csv_row(
            &[
                "--format=CSV",
                &format!("--timezone={tz}"),
                lat,
                lon,
                date,
                "sunrise",
            ],
            &[],
        );
        for field in ["sunrise", "transit", "sunset"] {
            assert!(row[field].ends_with(tz));
        }
    }
}

#[test]
fn test_sunrise_validation_and_show_inputs() {
    sunce_command()
        .args(["52.0", "13.4", "2024-06-21", "sunrise", "--horizon=invalid"])
        .assert()
        .failure();

    let (headers, _) = parse_csv_output(&output_text(
        &[
            "--format=CSV",
            "--show-inputs",
            "52.0",
            "13.4",
            "2024-06-21",
            "sunrise",
        ],
        &[("TZ", "Europe/Berlin")],
    ));
    assert!(headers.contains(&"latitude".to_string()));
    assert!(headers.contains(&"longitude".to_string()));
}

#[test]
fn test_sunrise_dst_and_accuracy_regressions() {
    sunce_command()
        .env("TZ", "Europe/Berlin")
        .args(["52.0", "13.4", "2024-03-31", "sunrise"])
        .assert()
        .success();

    for (lat, lon, date, sunrise, transit, sunset) in [
        (
            "53.0",
            "13.0",
            "2024-06-21",
            "2024-06-21T04:41:52+02:00",
            "2024-06-21T13:09:54+02:00",
            "2024-06-21T21:37:55+02:00",
        ),
        (
            "52.0",
            "13.4",
            "2024-06-02",
            "2024-06-02T04:51:15+02:00",
            "2024-06-02T13:04:28+02:00",
            "2024-06-02T21:18:17+02:00",
        ),
    ] {
        let row = csv_row(
            &["--format=CSV", lat, lon, date, "sunrise"],
            &[("TZ", "Europe/Berlin")],
        );
        assert_time_close(&row["sunrise"], sunrise, 0);
        assert_time_close(&row["transit"], transit, 0);
        assert_time_close(&row["sunset"], sunset, 0);
    }
}

#[test]
fn test_sunrise_specific_date_vs_partial_date_behavior() {
    let single = csv_row(
        &["--format=CSV", "52.0", "13.4", "2024-01-01", "sunrise"],
        &[("TZ", "Europe/Berlin")],
    );
    assert_eq!(single.get("type").map(String::as_str), Some("NORMAL"));
    assert_eq!(single["dateTime"], "2024-01-01T00:00:00+01:00");

    let january = csv_rows(&["--format=CSV", "52.0", "13.4", "2024-01", "sunrise"], &[]);
    assert_eq!(january.len(), 31);
    assert_eq!(january[0]["latitude"], "52.00000");
    assert_eq!(january[0]["longitude"], "13.40000");
    assert!(january[0]["dateTime"].starts_with("2024-01-01"));
    assert!(january[30]["dateTime"].starts_with("2024-01-31"));

    let (position_headers, position_rows) = parse_csv_output(&output_text(
        &["--format=CSV", "52.0", "13.4", "2024-01-01", "position"],
        &[],
    ));
    assert_eq!(position_rows.len(), 24);
    for field in ["latitude", "longitude", "dateTime", "azimuth", "zenith"] {
        assert!(position_headers.contains(&field.to_string()));
    }
}
