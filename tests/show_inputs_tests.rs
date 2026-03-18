mod common;
use common::{fields, parse_csv_output, parse_json_output, sunce_command, write_text_file};

fn csv_headers(args: &[&str], stdin: Option<&str>) -> Vec<String> {
    let mut cmd = sunce_command();
    cmd.args(args);
    if let Some(stdin) = stdin {
        cmd.write_stdin(stdin);
    }
    let output = cmd.output().unwrap();
    assert!(output.status.success());
    parse_csv_output(&String::from_utf8(output.stdout).unwrap()).0
}

fn json_output(args: &[&str]) -> serde_json::Value {
    let output = sunce_command().args(args).output().unwrap();
    assert!(output.status.success());
    parse_json_output(&String::from_utf8(output.stdout).unwrap())
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

fn sunrise_show_inputs_headers() -> Vec<String> {
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
}

#[test]
fn test_position_auto_show_inputs_rules() {
    let minimal = fields(&["dateTime", "azimuth", "zenith"]);
    let full = position_show_inputs_headers();

    assert_eq!(
        csv_headers(
            &[
                "--format=CSV",
                "52.0",
                "13.4",
                "2024-06-21T12:00:00",
                "position"
            ],
            None,
        ),
        minimal
    );

    for args in [
        vec![
            "--format=CSV",
            "52:53:1",
            "13.4",
            "2024-06-21T12:00:00",
            "position",
        ],
        vec!["--format=CSV", "52.0", "13.4", "2024-06", "position"],
        vec!["--format=CSV", "52.0", "13.4", "2024-06-21", "position"],
    ] {
        assert_eq!(csv_headers(&args, None), full);
    }
}

#[test]
fn test_position_input_sources_auto_enable_show_inputs() {
    let dir = tempfile::tempdir().unwrap();
    let coords = dir.path().join("coords.txt");
    let paired = dir.path().join("paired.txt");
    write_text_file(&coords, "52.0,13.4\n");
    write_text_file(&paired, "52.0,13.4,2024-06-21T12:00:00\n");

    for (args, stdin) in [
        (
            vec![
                "--format=CSV",
                &format!("@{}", coords.display()),
                "2024-06-21T12:00:00",
                "position",
            ],
            None,
        ),
        (
            vec![
                "--format=CSV",
                &format!("@{}", paired.display()),
                "position",
            ],
            None,
        ),
        (
            vec!["--format=CSV", "@-", "2024-06-21T12:00:00", "position"],
            Some("52.0,13.4\n"),
        ),
    ] {
        assert_eq!(csv_headers(&args, stdin), position_show_inputs_headers());
    }
}

#[test]
fn test_position_show_inputs_overrides() {
    assert_eq!(
        csv_headers(
            &[
                "--format=CSV",
                "--no-show-inputs",
                "52:53:1",
                "13.4",
                "2024-06-21T12:00:00",
                "position",
            ],
            None,
        ),
        fields(&["dateTime", "azimuth", "zenith"])
    );

    assert_eq!(
        csv_headers(
            &[
                "--format=CSV",
                "--show-inputs",
                "52.0",
                "13.4",
                "2024-06-21T12:00:00",
                "position",
            ],
            None,
        ),
        position_show_inputs_headers()
    );
}

#[test]
fn test_refraction_fields_are_omitted_when_disabled() {
    assert_eq!(
        csv_headers(
            &[
                "--format=CSV",
                "--show-inputs",
                "--no-refraction",
                "52.0",
                "13.4",
                "2024-06-21T12:00:00",
                "position",
            ],
            None,
        ),
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

    let json = json_output(&[
        "--format=JSON",
        "--show-inputs",
        "--no-refraction",
        "52.0",
        "13.4",
        "2024-06-21T12:00:00",
        "position",
    ]);
    assert!(json.get("pressure").is_none());
    assert!(json.get("temperature").is_none());
}

#[test]
fn test_position_json_single_values_do_not_auto_show_inputs() {
    let json = json_output(&[
        "--format=JSON",
        "52.0",
        "13.4",
        "2024-06-21T12:00:00",
        "position",
    ]);
    assert!(json.get("dateTime").is_some());
    assert!(json.get("azimuth").is_some());
    assert!(json.get("zenith").is_some());
    assert!(json.get("latitude").is_none());
    assert!(json.get("longitude").is_none());
}

#[test]
fn test_sunrise_show_inputs_rules() {
    let minimal = fields(&["dateTime", "type", "sunrise", "transit", "sunset"]);
    let full = sunrise_show_inputs_headers();

    for args in [
        vec!["--format=CSV", "52.0", "13.4", "2024-06-21", "sunrise"],
        vec![
            "--format=CSV",
            "52.0",
            "13.4",
            "2024-06-21",
            "sunrise",
            "--twilight",
        ],
    ] {
        let headers = csv_headers(&args, None);
        assert_eq!(&headers[..5], minimal.as_slice());
        assert!(!headers.contains(&"latitude".to_string()));
    }

    for args in [
        vec!["--format=CSV", "52.0", "13.4", "2024-06", "sunrise"],
        vec!["--format=CSV", "52:53:1", "13.4", "2024-06-21", "sunrise"],
        vec![
            "--format=CSV",
            "52:53:1",
            "13.4",
            "2024-06-21",
            "sunrise",
            "--twilight",
        ],
    ] {
        let headers = csv_headers(&args, None);
        assert!(headers.contains(&"latitude".to_string()));
        assert!(headers.contains(&"longitude".to_string()));
        assert!(headers.contains(&"dateTime".to_string()));
        assert!(headers.contains(&"deltaT".to_string()));
        if !args.contains(&"--twilight") {
            assert_eq!(headers, full);
        }
    }
}
