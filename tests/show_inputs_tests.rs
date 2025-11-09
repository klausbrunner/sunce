mod common;
use common::sunce_command;
use predicates::prelude::*;

/// Test that single-value inputs do NOT auto-enable show-inputs
#[test]
fn test_single_values_no_auto_show_inputs_csv() {
    let mut cmd = sunce_command();
    cmd.arg("--format=CSV")
        .arg("52.0")
        .arg("13.4")
        .arg("2024-06-21T12:00:00")
        .arg("position");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("dateTime,azimuth,zenith"))
        .stdout(predicate::str::contains("latitude").not())
        .stdout(predicate::str::contains("longitude").not());
}

#[test]
fn test_single_values_no_auto_show_inputs_json() {
    let mut cmd = sunce_command();
    cmd.arg("--format=JSON")
        .arg("52.0")
        .arg("13.4")
        .arg("2024-06-21T12:00:00")
        .arg("position");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains(r#""dateTime""#))
        .stdout(predicate::str::contains(r#""azimuth""#))
        .stdout(predicate::str::contains(r#""zenith""#))
        .stdout(predicate::str::contains(r#""latitude""#).not())
        .stdout(predicate::str::contains(r#""longitude""#).not());
}

/// Test that coordinate ranges DO auto-enable show-inputs
#[test]
fn test_coordinate_range_auto_enables_show_inputs() {
    let mut cmd = sunce_command();
    cmd.arg("--format=CSV")
        .arg("52:53:1")
        .arg("13.4")
        .arg("2024-06-21T12:00:00")
        .arg("position");

    cmd.assert().success().stdout(predicate::str::contains(
        "latitude,longitude,elevation,pressure,temperature,dateTime,deltaT,azimuth,zenith",
    ));
}

/// Test that time series (partial dates) DO auto-enable show-inputs
#[test]
fn test_partial_date_auto_enables_show_inputs() {
    let mut cmd = sunce_command();
    cmd.arg("--format=CSV")
        .arg("52.0")
        .arg("13.4")
        .arg("2024-06")
        .arg("position");

    cmd.assert().success().stdout(predicate::str::contains(
        "latitude,longitude,elevation,pressure,temperature,dateTime,deltaT,azimuth,zenith",
    ));
}

/// Test that file inputs DO auto-enable show-inputs
#[test]
fn test_file_input_auto_enables_show_inputs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let file_path = temp_dir.path().join("coords.txt");
    std::fs::write(&file_path, "52.0,13.4\n").unwrap();

    let mut cmd = sunce_command();
    cmd.arg("--format=CSV")
        .arg(format!("@{}", file_path.display()))
        .arg("2024-06-21T12:00:00")
        .arg("position");

    cmd.assert().success().stdout(predicate::str::contains(
        "latitude,longitude,elevation,pressure,temperature,dateTime,deltaT,azimuth,zenith",
    ));
}

/// Test that stdin input DO auto-enable show-inputs
#[test]
fn test_stdin_auto_enables_show_inputs() {
    let mut cmd = sunce_command();
    cmd.arg("--format=CSV")
        .arg("@-")
        .arg("2024-06-21T12:00:00")
        .arg("position")
        .write_stdin("52.0,13.4\n");

    cmd.assert().success().stdout(predicate::str::contains(
        "latitude,longitude,elevation,pressure,temperature,dateTime,deltaT,azimuth,zenith",
    ));
}

/// Test that paired file inputs DO auto-enable show-inputs
#[test]
fn test_paired_file_auto_enables_show_inputs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let file_path = temp_dir.path().join("paired.txt");
    std::fs::write(&file_path, "52.0,13.4,2024-06-21T12:00:00\n").unwrap();

    let mut cmd = sunce_command();
    cmd.arg("--format=CSV")
        .arg(format!("@{}", file_path.display()))
        .arg("position");

    cmd.assert().success().stdout(predicate::str::contains(
        "latitude,longitude,elevation,pressure,temperature,dateTime,deltaT,azimuth,zenith",
    ));
}

/// Test that --no-show-inputs overrides auto-enable
#[test]
fn test_no_show_inputs_override() {
    let mut cmd = sunce_command();
    cmd.arg("--format=CSV")
        .arg("--no-show-inputs")
        .arg("52:53:1")
        .arg("13.4")
        .arg("2024-06-21T12:00:00")
        .arg("position");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("dateTime,azimuth,zenith"))
        .stdout(predicate::str::contains("latitude").not());
}

/// Test that explicit --show-inputs works for single values
#[test]
fn test_explicit_show_inputs_for_single_values() {
    let mut cmd = sunce_command();
    cmd.arg("--format=CSV")
        .arg("--show-inputs")
        .arg("52.0")
        .arg("13.4")
        .arg("2024-06-21T12:00:00")
        .arg("position");

    cmd.assert().success().stdout(predicate::str::contains(
        "latitude,longitude,elevation,pressure,temperature,dateTime,deltaT,azimuth,zenith",
    ));
}

/// Test sunrise single values do NOT auto-enable show-inputs
#[test]
fn test_sunrise_single_values_no_auto_show_inputs() {
    let mut cmd = sunce_command();
    cmd.arg("--format=CSV")
        .arg("52.0")
        .arg("13.4")
        .arg("2024-06-21")
        .arg("sunrise");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains(
            "dateTime,type,sunrise,transit,sunset",
        ))
        .stdout(predicate::str::contains("latitude").not())
        .stdout(predicate::str::contains("longitude").not());
}

/// Test sunrise with partial date DOES auto-enable show-inputs
#[test]
fn test_sunrise_partial_date_auto_enables_show_inputs() {
    let mut cmd = sunce_command();
    cmd.arg("--format=CSV")
        .arg("52.0")
        .arg("13.4")
        .arg("2024-06")
        .arg("sunrise");

    cmd.assert().success().stdout(predicate::str::contains(
        "latitude,longitude,dateTime,deltaT,type,sunrise,transit,sunset",
    ));
}

/// Test sunrise with coordinate range DOES auto-enable show-inputs
#[test]
fn test_sunrise_coordinate_range_auto_enables_show_inputs() {
    let mut cmd = sunce_command();
    cmd.arg("--format=CSV")
        .arg("52:53:1")
        .arg("13.4")
        .arg("2024-06-21")
        .arg("sunrise");

    cmd.assert().success().stdout(predicate::str::contains(
        "latitude,longitude,dateTime,deltaT,type,sunrise,transit,sunset",
    ));
}

/// Test twilight single values do NOT auto-enable show-inputs
#[test]
fn test_twilight_single_values_no_auto_show_inputs() {
    let mut cmd = sunce_command();
    cmd.arg("--format=CSV")
        .arg("52.0")
        .arg("13.4")
        .arg("2024-06-21")
        .arg("sunrise")
        .arg("--twilight");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains(
            "dateTime,type,sunrise,transit,sunset",
        ))
        .stdout(predicate::str::contains("latitude").not());
}

/// Test twilight with range DOES auto-enable show-inputs
#[test]
fn test_twilight_range_auto_enables_show_inputs() {
    let mut cmd = sunce_command();
    cmd.arg("--format=CSV")
        .arg("52:53:1")
        .arg("13.4")
        .arg("2024-06-21")
        .arg("sunrise")
        .arg("--twilight");

    cmd.assert().success().stdout(predicate::str::contains(
        "latitude,longitude,dateTime,deltaT",
    ));
}

/// Test position command with complete date (YYYY-MM-DD) DOES auto-enable show-inputs
/// because it expands to time series
#[test]
fn test_position_complete_date_auto_enables_show_inputs() {
    let mut cmd = sunce_command();
    cmd.arg("--format=CSV")
        .arg("52.0")
        .arg("13.4")
        .arg("2024-06-21")
        .arg("position");

    cmd.assert().success().stdout(predicate::str::contains(
        "latitude,longitude,elevation,pressure,temperature,dateTime,deltaT,azimuth,zenith",
    ));
}

/// Test sunrise command with complete date (YYYY-MM-DD) does NOT auto-enable show-inputs
/// because it's a single sunrise calculation
#[test]
fn test_sunrise_complete_date_no_auto_show_inputs() {
    let mut cmd = sunce_command();
    cmd.arg("--format=CSV")
        .arg("52.0")
        .arg("13.4")
        .arg("2024-06-21")
        .arg("sunrise");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains(
            "dateTime,type,sunrise,transit,sunset",
        ))
        .stdout(predicate::str::contains("latitude").not());
}
