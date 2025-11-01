use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn json_position_uses_elevation_angle_label() {
    let mut cmd = Command::cargo_bin("sunce").unwrap();
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
        .stdout(predicate::str::contains("\"elevation\":"))
        .stdout(predicate::str::contains("\"zenith\"").not());
}
