/// Critical CI tests for production reliability
mod common;
use common::*;

fn csv_number_field(stdout: &str, field: &str) -> f64 {
    parse_csv_single_record_map(stdout)[field]
        .parse::<f64>()
        .unwrap_or_else(|_| panic!("missing or invalid numeric field: {field}"))
}

fn csv_string_field(stdout: &str, field: &str) -> String {
    parse_csv_single_record_map(stdout)[field].clone()
}

#[test]
fn test_exact_position_baseline() {
    let stdout = String::from_utf8(
        SunceTest::new()
            .args([
                "--format=CSV",
                "--show-inputs",
                "52.0",
                "13.4",
                "2024-06-21T12:00:00+02:00",
                "position",
            ])
            .get_output()
            .stdout,
    )
    .unwrap();

    let (headers, _) = parse_csv_output(&stdout);
    assert_eq!(
        headers,
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
    assert_eq!(csv_string_field(&stdout, "latitude"), "52.00000");
    assert_eq!(csv_string_field(&stdout, "longitude"), "13.40000");
    assert!(csv_string_field(&stdout, "dateTime").starts_with("2024-06-21T12:00:00"));
    assert!(csv_string_field(&stdout, "dateTime").ends_with("+02:00"));
    assert!((csv_number_field(&stdout, "azimuth") - 148.8808).abs() <= 1e-4);
    assert!((csv_number_field(&stdout, "zenith") - 31.4083).abs() <= 1e-4);
}

#[test]
fn test_exact_sunrise_baseline() {
    let stdout = String::from_utf8(
        SunceTest::new()
            .args([
                "--format=CSV",
                "--show-inputs",
                "--timezone=UTC",
                "52.0",
                "13.4",
                "2024-06-21",
                "sunrise",
            ])
            .get_output()
            .stdout,
    )
    .unwrap();

    let (headers, _) = parse_csv_output(&stdout);
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
    assert_eq!(csv_string_field(&stdout, "latitude"), "52.00000");
    assert_eq!(csv_string_field(&stdout, "longitude"), "13.40000");
    assert_eq!(csv_string_field(&stdout, "type"), "NORMAL");
    assert_time_close(
        &csv_string_field(&stdout, "sunrise"),
        "2024-06-21T02:46:15+00:00",
        0,
    );
    assert_time_close(
        &csv_string_field(&stdout, "transit"),
        "2024-06-21T11:08:18+00:00",
        0,
    );
    assert_time_close(
        &csv_string_field(&stdout, "sunset"),
        "2024-06-21T19:30:20+00:00",
        0,
    );
}

#[test]
fn test_json_output_structure() {
    let stdout = String::from_utf8(
        SunceTest::new()
            .args([
                "--format=JSON",
                "--timezone=UTC",
                "52.0",
                "13.4",
                "2024-01-01T12:00:00",
                "position",
            ])
            .get_output()
            .stdout,
    )
    .unwrap();

    let json = parse_json_output(&stdout);
    assert!(json.get("dateTime").is_some());
    assert!(json.get("azimuth").is_some());
    assert!(json.get("zenith").is_some());
    let azimuth = json
        .get("azimuth")
        .and_then(serde_json::Value::as_f64)
        .unwrap();
    let zenith = json
        .get("zenith")
        .and_then(serde_json::Value::as_f64)
        .unwrap();
    assert!((191.0..=192.0).contains(&azimuth));
    assert!((75.0..=76.0).contains(&zenith));
}

#[test]
fn test_failure_paths() {
    for args in [
        vec!["999", "13.4", "2024-01-01T12:00:00", "position"],
        vec!["52.0", "999", "2024-01-01T12:00:00", "position"],
        vec!["52.0", "13.4", "invalid-date", "position"],
        vec!["52.0", "13.4", "2024-01-01T12:00:00", "invalid"],
        vec!["52.0"],
        vec![
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "--algorithm=INVALID",
        ],
        vec![
            "--format=INVALID",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ],
        vec!["@nonexistent.txt", "position"],
    ] {
        SunceTest::new().args(args).assert_failure();
    }
}

#[test]
fn test_cli_precedence_and_deltat() {
    SunceTest::new()
        .args([
            "--format=CSV",
            "--show-inputs",
            "--deltat=69.2",
            "--deltat",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ])
        .assert_failure();

    let explicit = String::from_utf8(
        SunceTest::new()
            .args([
                "--format=CSV",
                "--show-inputs",
                "--deltat=69.2",
                "52.0",
                "13.4",
                "2024-01-01T12:00:00",
                "position",
            ])
            .get_output()
            .stdout,
    )
    .unwrap();
    assert_eq!(csv_string_field(&explicit, "deltaT"), "69.200");

    let estimated = String::from_utf8(
        SunceTest::new()
            .args([
                "--format=CSV",
                "--show-inputs",
                "--deltat",
                "52.0",
                "13.4",
                "2024-01-01T12:00:00",
                "position",
            ])
            .get_output()
            .stdout,
    )
    .unwrap();
    let delta_t = csv_number_field(&estimated, "deltaT");
    assert!((70.0..=72.0).contains(&delta_t));
    assert_ne!(delta_t, 69.2);

    let default = String::from_utf8(
        SunceTest::new()
            .args([
                "--format=CSV",
                "--show-inputs",
                "52.0",
                "13.4",
                "2024-06-21T12:00:00+02:00",
                "position",
            ])
            .get_output()
            .stdout,
    )
    .unwrap();
    assert_eq!(csv_number_field(&default, "deltaT"), 0.0);
}

#[test]
fn test_show_inputs_and_option_positioning() {
    let stdout = String::from_utf8(
        SunceTest::new()
            .args([
                "--format=CSV",
                "--no-show-inputs",
                "52:53:1",
                "13.4",
                "2024-01-01T12:00:00",
                "position",
            ])
            .get_output()
            .stdout,
    )
    .unwrap();
    let (headers, rows) = parse_csv_output(&stdout);
    assert!(!headers.contains(&"latitude".to_string()));
    assert!(!headers.contains(&"longitude".to_string()));
    assert!(headers.contains(&"azimuth".to_string()));
    assert!(headers.contains(&"zenith".to_string()));
    assert!(rows.len() >= 2);

    for args in [
        vec![
            "--format=CSV",
            "--timezone=UTC",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "--algorithm=SPA",
        ],
        vec![
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "--algorithm=SPA",
            "--elevation-angle",
        ],
    ] {
        SunceTest::new().args(args).assert_success();
    }
}

#[test]
fn test_parameter_validation_errors() {
    for args in [
        vec![
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "--elevation=invalid",
        ],
        vec![
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "--pressure=not_a_number",
        ],
        vec![
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
            "--temperature=xyz",
        ],
        vec![
            "--deltat=invalid",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ],
    ] {
        SunceTest::new().args(args).assert_failure();
    }
}
