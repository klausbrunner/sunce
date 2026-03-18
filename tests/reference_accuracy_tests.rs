mod common;
use common::{load_fixture_rows, parse_csv_single_record_map, sunce_command};

fn assert_close(actual: f64, expected: f64, tolerance: f64, field: &str, case: &str) {
    let delta = (actual - expected).abs();
    assert!(
        delta <= tolerance,
        "{case}: {field} mismatch: actual={actual}, expected={expected}, tolerance={tolerance}"
    );
}

fn run_position_case(case: &std::collections::HashMap<String, String>) {
    let mut cmd = sunce_command();
    cmd.env("TZ", "UTC").args([
        "--format=CSV",
        case.get("latitude").unwrap(),
        case.get("longitude").unwrap(),
        case.get("dateTime").unwrap(),
        "position",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();
    let record = parse_csv_single_record_map(&String::from_utf8(output).unwrap());
    let case_name = case.get("case").unwrap();
    let azimuth = record["azimuth"]
        .parse::<f64>()
        .expect("azimuth should parse");
    let zenith = record["zenith"]
        .parse::<f64>()
        .expect("zenith should parse");
    let expected_azimuth = case["azimuth"]
        .parse::<f64>()
        .expect("fixture azimuth should parse");
    let expected_zenith = case["zenith"]
        .parse::<f64>()
        .expect("fixture zenith should parse");

    assert_close(azimuth, expected_azimuth, 1e-4, "azimuth", case_name);
    assert_close(zenith, expected_zenith, 1e-4, "zenith", case_name);
}

#[test]
fn test_position_reference_accuracy_from_fixture() {
    for case in load_fixture_rows("position_oracles.csv")
        .into_iter()
        .filter(|case| {
            matches!(
                case.get("case").map(String::as_str),
                Some("greenwich_noon")
                    | Some("winter_solstice_us")
                    | Some("equinox_equator")
                    | Some("sydney_summer")
            )
        })
    {
        run_position_case(&case);
    }
}
