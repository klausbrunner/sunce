mod common;
use common::{fields, parse_csv_output, parse_json_output, sunce_command};

fn csv_headers_for_args(args: &[&str]) -> Vec<String> {
    let mut cmd = sunce_command();
    cmd.args(args);
    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let (headers, _rows) = parse_csv_output(&stdout);
    headers
}

fn position_show_inputs_headers() -> Vec<String> {
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
}

/// Test that single-value inputs do NOT auto-enable show-inputs
#[test]
fn test_single_values_no_auto_show_inputs_csv() {
    let headers = csv_headers_for_args(&[
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-06-21T12:00:00",
        "position",
    ]);

    assert_eq!(headers, fields(&["dateTime", "azimuth", "zenith"]));
}

#[test]
fn test_single_values_no_auto_show_inputs_json() {
    let mut cmd = sunce_command();
    cmd.args([
        "--format=JSON",
        "52.0",
        "13.4",
        "2024-06-21T12:00:00",
        "position",
    ]);

    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let json = parse_json_output(&stdout);
    assert!(json.get("dateTime").is_some());
    assert!(json.get("azimuth").is_some());
    assert!(json.get("zenith").is_some());
    assert!(json.get("latitude").is_none());
    assert!(json.get("longitude").is_none());
}

/// Test that coordinate ranges DO auto-enable show-inputs
#[test]
fn test_coordinate_range_auto_enables_show_inputs() {
    let headers = csv_headers_for_args(&[
        "--format=CSV",
        "52:53:1",
        "13.4",
        "2024-06-21T12:00:00",
        "position",
    ]);
    assert_eq!(headers, position_show_inputs_headers());
}

/// Test that time series (partial dates) DO auto-enable show-inputs
#[test]
fn test_partial_date_auto_enables_show_inputs() {
    let headers = csv_headers_for_args(&["--format=CSV", "52.0", "13.4", "2024-06", "position"]);
    assert_eq!(headers, position_show_inputs_headers());
}

/// Test that file inputs DO auto-enable show-inputs
#[test]
fn test_file_input_auto_enables_show_inputs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let file_path = temp_dir.path().join("coords.txt");
    std::fs::write(&file_path, "52.0,13.4\n").unwrap();

    let headers = csv_headers_for_args(&[
        "--format=CSV",
        &format!("@{}", file_path.display()),
        "2024-06-21T12:00:00",
        "position",
    ]);

    assert_eq!(headers, position_show_inputs_headers());
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

    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let (headers, _rows) = parse_csv_output(&stdout);
    assert_eq!(headers, position_show_inputs_headers());
}

/// Test that paired file inputs DO auto-enable show-inputs
#[test]
fn test_paired_file_auto_enables_show_inputs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let file_path = temp_dir.path().join("paired.txt");
    std::fs::write(&file_path, "52.0,13.4,2024-06-21T12:00:00\n").unwrap();

    let headers = csv_headers_for_args(&[
        "--format=CSV",
        &format!("@{}", file_path.display()),
        "position",
    ]);

    assert_eq!(headers, position_show_inputs_headers());
}

/// Test that --no-show-inputs overrides auto-enable
#[test]
fn test_no_show_inputs_override() {
    let headers = csv_headers_for_args(&[
        "--format=CSV",
        "--no-show-inputs",
        "52:53:1",
        "13.4",
        "2024-06-21T12:00:00",
        "position",
    ]);

    assert_eq!(headers, fields(&["dateTime", "azimuth", "zenith"]));
}

/// Test that explicit --show-inputs works for single values
#[test]
fn test_explicit_show_inputs_for_single_values() {
    let headers = csv_headers_for_args(&[
        "--format=CSV",
        "--show-inputs",
        "52.0",
        "13.4",
        "2024-06-21T12:00:00",
        "position",
    ]);
    assert_eq!(headers, position_show_inputs_headers());
}

/// Test that refraction parameters are omitted when refraction is disabled
#[test]
fn test_show_inputs_omits_refraction_fields_when_disabled_csv() {
    let headers = csv_headers_for_args(&[
        "--format=CSV",
        "--show-inputs",
        "--no-refraction",
        "52.0",
        "13.4",
        "2024-06-21T12:00:00",
        "position",
    ]);

    assert_eq!(
        headers,
        fields(&[
            "latitude",
            "longitude",
            "elevation",
            "dateTime",
            "deltaT",
            "azimuth",
            "zenith",
        ])
    );
}

/// Test JSON output also omits refraction fields when disabled
#[test]
fn test_show_inputs_omits_refraction_fields_when_disabled_json() {
    let mut cmd = sunce_command();
    cmd.args([
        "--format=JSON",
        "--show-inputs",
        "--no-refraction",
        "52.0",
        "13.4",
        "2024-06-21T12:00:00",
        "position",
    ]);

    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let json = parse_json_output(&stdout);
    assert!(json.get("pressure").is_none());
    assert!(json.get("temperature").is_none());
}

/// Test sunrise single values do NOT auto-enable show-inputs
#[test]
fn test_sunrise_single_values_no_auto_show_inputs() {
    let headers = csv_headers_for_args(&["--format=CSV", "52.0", "13.4", "2024-06-21", "sunrise"]);

    assert_eq!(
        headers,
        fields(&["dateTime", "type", "sunrise", "transit", "sunset"])
    );
}

/// Test sunrise with partial date DOES auto-enable show-inputs
#[test]
fn test_sunrise_partial_date_auto_enables_show_inputs() {
    let headers = csv_headers_for_args(&["--format=CSV", "52.0", "13.4", "2024-06", "sunrise"]);

    assert_eq!(
        headers,
        fields(&[
            "latitude",
            "longitude",
            "dateTime",
            "deltaT",
            "type",
            "sunrise",
            "transit",
            "sunset",
        ])
    );
}

/// Test sunrise with coordinate range DOES auto-enable show-inputs
#[test]
fn test_sunrise_coordinate_range_auto_enables_show_inputs() {
    let headers =
        csv_headers_for_args(&["--format=CSV", "52:53:1", "13.4", "2024-06-21", "sunrise"]);

    assert_eq!(
        headers,
        fields(&[
            "latitude",
            "longitude",
            "dateTime",
            "deltaT",
            "type",
            "sunrise",
            "transit",
            "sunset",
        ])
    );
}

/// Test twilight single values do NOT auto-enable show-inputs
#[test]
fn test_twilight_single_values_no_auto_show_inputs() {
    let headers = csv_headers_for_args(&[
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-06-21",
        "sunrise",
        "--twilight",
    ]);

    assert_eq!(
        headers[0..5],
        fields(&["dateTime", "type", "sunrise", "transit", "sunset"])
    );
    assert!(!headers.contains(&"latitude".to_string()));
}

/// Test twilight with range DOES auto-enable show-inputs
#[test]
fn test_twilight_range_auto_enables_show_inputs() {
    let headers = csv_headers_for_args(&[
        "--format=CSV",
        "52:53:1",
        "13.4",
        "2024-06-21",
        "sunrise",
        "--twilight",
    ]);

    assert!(headers.contains(&"latitude".to_string()));
    assert!(headers.contains(&"longitude".to_string()));
    assert!(headers.contains(&"dateTime".to_string()));
    assert!(headers.contains(&"deltaT".to_string()));
}

/// Test position command with complete date (YYYY-MM-DD) DOES auto-enable show-inputs
/// because it expands to time series
#[test]
fn test_position_complete_date_auto_enables_show_inputs() {
    let headers = csv_headers_for_args(&["--format=CSV", "52.0", "13.4", "2024-06-21", "position"]);
    assert_eq!(headers, position_show_inputs_headers());
}

/// Test sunrise command with complete date (YYYY-MM-DD) does NOT auto-enable show-inputs
/// because it's a single sunrise calculation
#[test]
fn test_sunrise_complete_date_no_auto_show_inputs() {
    let headers = csv_headers_for_args(&["--format=CSV", "52.0", "13.4", "2024-06-21", "sunrise"]);
    assert_eq!(
        headers,
        fields(&["dateTime", "type", "sunrise", "transit", "sunset"])
    );
}
