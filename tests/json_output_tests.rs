mod common;
use common::parse_json_output;
use common::sunce_command;

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
