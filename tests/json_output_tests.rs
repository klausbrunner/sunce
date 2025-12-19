mod common;
use common::sunce_command;
use predicates::prelude::*;

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

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("\"elevation-angle\":"))
        .stdout(predicate::str::contains("\"zenith\"").not());
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

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("\"elevation\":"))
        .stdout(predicate::str::contains("\"elevation-angle\":"))
        .stdout(predicate::str::contains("\"deltaT\":"));
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

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("\"deltaT\"").not());
}
