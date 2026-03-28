mod common;
use common::{parse_csv_output, parse_json_output, sunce_command};

fn output_text(args: &[&str]) -> String {
    let output = sunce_command().args(args).output().unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout).unwrap()
}

fn json_keys(args: &[&str]) -> Vec<String> {
    let mut keys = parse_json_output(&output_text(args))
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    keys.sort();
    keys
}

fn csv_headers(args: &[&str]) -> Vec<String> {
    let (headers, _) = parse_csv_output(&output_text(args));
    headers
}

#[test]
fn json_position_uses_elevation_angle_label() {
    let mut cmd = sunce_command();
    cmd.args([
        "--format=json",
        "--elevation-angle",
        "52.0",
        "13.4",
        "2024-06-21T12:00:00",
        "position",
    ]);

    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let json = parse_json_output(&stdout);
    assert!(json.get("elevation-angle").is_some());
    assert!(json.get("zenith").is_none());
}

#[test]
fn json_position_show_inputs_includes_site_elevation_and_angle() {
    let mut cmd = sunce_command();
    cmd.args([
        "--format=json",
        "--show-inputs",
        "--elevation-angle",
        "--elevation=123.0",
        "52.0",
        "13.4",
        "2024-06-21T12:00:00",
        "position",
    ]);

    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let json = parse_json_output(&stdout);
    assert!(json.get("elevation").is_some());
    assert!(json.get("elevation-angle").is_some());
    assert!(json.get("deltaT").is_some());
}

#[test]
fn json_position_without_show_inputs_omits_delta_t() {
    let mut cmd = sunce_command();
    cmd.args([
        "--format=json",
        "52.0",
        "13.4",
        "2024-06-21T12:00:00",
        "position",
    ]);

    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let json = parse_json_output(&stdout);
    assert!(json.get("deltaT").is_none());
}

#[test]
fn json_sunrise_without_twilight_has_expected_fields() {
    let json = parse_json_output(&output_text(&[
        "--format=json",
        "52.0",
        "13.4",
        "2024-06-21",
        "sunrise",
    ]));

    let keys = json.as_object().unwrap();
    assert!(keys.contains_key("dateTime"));
    assert!(keys.contains_key("type"));
    assert!(keys.contains_key("sunrise"));
    assert!(keys.contains_key("transit"));
    assert!(keys.contains_key("sunset"));
    assert!(!keys.contains_key("latitude"));
    assert!(!keys.contains_key("longitude"));
    assert!(!keys.contains_key("deltaT"));
    assert!(!keys.contains_key("civil_start"));
}

#[test]
fn position_csv_headers_and_json_keys_match() {
    let csv = csv_headers(&[
        "--format=csv",
        "--show-inputs",
        "--elevation-angle",
        "52.0",
        "13.4",
        "2024-06-21T12:00:00",
        "position",
    ]);
    let json = json_keys(&[
        "--format=json",
        "--show-inputs",
        "--elevation-angle",
        "52.0",
        "13.4",
        "2024-06-21T12:00:00",
        "position",
    ]);

    let mut csv = csv;
    csv.sort();
    assert_eq!(csv, json);
}

#[test]
fn sunrise_csv_headers_and_json_keys_match() {
    let csv = csv_headers(&[
        "--format=csv",
        "--show-inputs",
        "52.0",
        "13.4",
        "2024-06-21",
        "sunrise",
        "--twilight",
    ]);
    let json = json_keys(&[
        "--format=json",
        "--show-inputs",
        "52.0",
        "13.4",
        "2024-06-21",
        "sunrise",
        "--twilight",
    ]);

    let mut csv = csv;
    csv.sort();
    assert_eq!(csv, json);
}
