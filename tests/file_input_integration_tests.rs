mod common;
use common::{
    find_csv_row, load_fixture_rows, parse_csv_output_maps, sunce_command, write_text_file,
};
use predicates::prelude::*;
use std::collections::HashMap;
use std::path::Path;
use tempfile::tempdir;

fn output_text(args: &[&str], stdin: Option<&str>, envs: &[(&str, &str)]) -> String {
    let mut cmd = sunce_command();
    for (key, value) in envs {
        cmd.env(key, value);
    }
    cmd.args(args);
    if let Some(stdin) = stdin {
        cmd.write_stdin(stdin);
    }
    String::from_utf8(cmd.assert().success().get_output().stdout.clone()).unwrap()
}

fn csv_records(
    args: &[&str],
    stdin: Option<&str>,
    envs: &[(&str, &str)],
) -> Vec<HashMap<String, String>> {
    parse_csv_output_maps(&output_text(args, stdin, envs))
}

fn assert_fields(row: &HashMap<String, String>, expected: &[(&str, &str)]) {
    for (field, expected_value) in expected {
        assert_eq!(row.get(*field).map(String::as_str), Some(*expected_value));
    }
}

fn assert_field_values(rows: &[HashMap<String, String>], field: &str, expected_values: &[&str]) {
    for expected in expected_values {
        assert!(
            rows.iter()
                .any(|row| row.get(field).map(String::as_str) == Some(*expected)),
            "missing {field}={expected} in {rows:?}"
        );
    }
}

fn assert_field_prefixes(rows: &[HashMap<String, String>], field: &str, prefixes: &[&str]) {
    for prefix in prefixes {
        assert!(
            rows.iter().any(|row| row
                .get(field)
                .is_some_and(|value| value.starts_with(prefix))),
            "missing {field} prefix {prefix} in {rows:?}"
        );
    }
}

fn fixture_row(file: &str, case: &str) -> HashMap<String, String> {
    load_fixture_rows(file)
        .into_iter()
        .find(|row| row.get("case").map(String::as_str) == Some(case))
        .unwrap_or_else(|| panic!("missing fixture {file}:{case}"))
}

fn assert_oracle_fields(
    rows: &[HashMap<String, String>],
    filters: &[(&str, &str)],
    oracle: &HashMap<String, String>,
    fields: &[&str],
) {
    let expected = fields
        .iter()
        .map(|field| (*field, oracle[*field].as_str()))
        .collect::<Vec<_>>();
    assert_fields(find_csv_row(rows, filters), &expected);
}

fn file_arg(dir: &Path, name: &str, contents: &str) -> String {
    let path = dir.join(name);
    write_text_file(&path, contents);
    format!("@{}", path.display())
}

#[test]
fn test_file_comments_and_mixed_delimiters() {
    let dir = tempdir().unwrap();
    let comments = file_arg(
        dir.path(),
        "comments.txt",
        "# comment\n52.0,13.4\n\n# another\n59.334,18.063\n",
    );
    let mixed = file_arg(
        dir.path(),
        "mixed_coords.txt",
        "52.0,13.4\n59.334 18.063\n40.42 -3.70\n",
    );

    let rows = csv_records(
        &["--format=CSV", &comments, "2024-06-21T12:00:00", "position"],
        None,
        &[],
    );
    assert_eq!(rows.len(), 2);

    let rows = csv_records(
        &["--format=CSV", &mixed, "2024-06-21T12:00:00", "position"],
        None,
        &[],
    );
    assert_eq!(rows.len(), 3);
    assert_field_values(&rows, "longitude", &["-3.70000"]);
}

#[test]
fn test_basic_sunrise_file_inputs() {
    let dir = tempdir().unwrap();
    let paired = file_arg(
        dir.path(),
        "paired.txt",
        "52.0,13.4,2024-06-21\n40.42,-3.70,2024-12-21\n",
    );

    let rows = csv_records(&["--format=CSV", &paired, "sunrise"], None, &[]);
    assert_eq!(rows.len(), 2);
    assert_field_prefixes(&rows, "dateTime", &["2024-06-21", "2024-12-21"]);
}

#[test]
fn test_file_input_error_cases() {
    let dir = tempdir().unwrap();
    let missing_file = dir.path().join("missing_times.txt");
    let invalid_times = file_arg(
        dir.path(),
        "bad_times.txt",
        "2024-06-21T12:00:00\nnot-a-timestamp\n",
    );
    let invalid_coords = file_arg(dir.path(), "invalid.txt", "invalid,data\n");

    for (args, stderr) in [
        (
            vec![
                "52.0".to_string(),
                "13.4".to_string(),
                format!("@{}", missing_file.display()),
                "position".to_string(),
            ],
            "Error opening",
        ),
        (
            vec![
                "52.0".to_string(),
                "13.4".to_string(),
                invalid_times.clone(),
                "position".to_string(),
            ],
            "bad_times.txt:2",
        ),
        (
            vec![
                invalid_coords.clone(),
                "2024-06-21".to_string(),
                "position".to_string(),
            ],
            "invalid latitude",
        ),
    ] {
        let refs = args.iter().map(String::as_str).collect::<Vec<_>>();
        sunce_command()
            .args(&refs)
            .assert()
            .failure()
            .stderr(predicate::str::contains(stderr));
    }
}

#[test]
fn test_file_input_timezone_override() {
    let dir = tempdir().unwrap();
    let mixed_times = file_arg(
        dir.path(),
        "mixed_times.txt",
        "2024-06-21T12:00:00+02:00\n2024-06-21T15:00:00\n2024-06-21T18:00:00Z\n",
    );

    let rows = csv_records(
        &[
            "--format=CSV",
            "--timezone=+05:00",
            "52.0",
            "13.4",
            &mixed_times,
            "position",
        ],
        None,
        &[],
    );
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|row| row["dateTime"].ends_with("+05:00")));
}

#[test]
fn test_position_file_input_oracles_from_fixtures() {
    let dir = tempdir().unwrap();
    let coords = file_arg(dir.path(), "coords.txt", "52.0,13.4\n59.334,18.063\n");
    let times = file_arg(
        dir.path(),
        "times.txt",
        "2024-06-21T12:00:00\n2024-06-21T18:00:00\n",
    );
    let paired = file_arg(
        dir.path(),
        "paired.txt",
        "52.0,13.4,2024-06-21T12:00:00\n40.42,-3.70,2024-12-21T12:00:00\n",
    );

    let berlin_noon = fixture_row("position_oracles.csv", "berlin_solstice_noon");
    let stockholm_noon = fixture_row("position_oracles.csv", "stockholm_solstice_noon");
    let berlin_evening = fixture_row("position_oracles.csv", "berlin_solstice_evening");
    let madrid_winter_noon = fixture_row("position_oracles.csv", "madrid_winter_noon");

    let rows = csv_records(
        &["--format=CSV", &coords, "2024-06-21T12:00:00", "position"],
        None,
        &[("TZ", "UTC")],
    );
    for oracle in [&berlin_noon, &stockholm_noon] {
        assert_oracle_fields(
            &rows,
            &[
                ("latitude", oracle["latitude"].as_str()),
                ("longitude", oracle["longitude"].as_str()),
            ],
            oracle,
            &["azimuth", "zenith"],
        );
    }

    let rows = csv_records(
        &["--format=CSV", "52.0", "13.4", &times, "position"],
        None,
        &[("TZ", "UTC")],
    );
    for oracle in [&berlin_noon, &berlin_evening] {
        assert_oracle_fields(
            &rows,
            &[("dateTime", oracle["dateTime"].as_str())],
            oracle,
            &["azimuth", "zenith"],
        );
    }

    let rows = csv_records(
        &["--format=CSV", &paired, "position"],
        None,
        &[("TZ", "UTC")],
    );
    for oracle in [&berlin_noon, &madrid_winter_noon] {
        assert_oracle_fields(
            &rows,
            &[
                ("latitude", oracle["latitude"].as_str()),
                ("dateTime", oracle["dateTime"].as_str()),
            ],
            oracle,
            &["azimuth", "zenith"],
        );
    }

    let rows = csv_records(
        &["--format=CSV", "@-", "position"],
        Some("52.0,13.4,2024-06-21T12:00:00\n"),
        &[("TZ", "UTC")],
    );
    assert_oracle_fields(
        &rows,
        &[("latitude", berlin_noon["latitude"].as_str())],
        &berlin_noon,
        &["longitude", "azimuth", "zenith"],
    );
}

#[test]
fn test_sunrise_file_input_oracles_from_fixtures() {
    let dir = tempdir().unwrap();
    let coords = file_arg(dir.path(), "coords.txt", "52.0,13.4\n40.42,-3.70\n");
    let berlin = fixture_row("sunrise_oracles.csv", "berlin_solstice");
    let madrid = fixture_row("sunrise_oracles.csv", "madrid_solstice");
    let rows = csv_records(
        &["--format=CSV", &coords, "2024-06-21", "sunrise"],
        None,
        &[("TZ", "UTC")],
    );

    for oracle in [&berlin, &madrid] {
        assert_oracle_fields(
            &rows,
            &[
                ("latitude", oracle["latitude"].as_str()),
                ("longitude", oracle["longitude"].as_str()),
            ],
            oracle,
            &["sunrise", "sunset"],
        );
    }
}

#[test]
fn test_cartesian_products_with_file_inputs() {
    let dir = tempdir().unwrap();
    let times = file_arg(
        dir.path(),
        "times.txt",
        "2024-06-21T12:00:00\n2024-06-21T18:00:00\n",
    );
    let coords = file_arg(dir.path(), "coords.txt", "52.0,13.4\n53.0,14.4\n");

    for (args, stdin) in [
        (vec!["--format=CSV", &coords, &times, "position"], None),
        (
            vec!["--format=CSV", "@-", &times, "position"],
            Some("52.0,13.4\n53.0,14.4\n"),
        ),
        (
            vec!["--format=CSV", &coords, "@-", "position"],
            Some("2024-06-21T12:00:00\n2024-06-21T18:00:00\n"),
        ),
    ] {
        let rows = csv_records(&args, stdin, &[("TZ", "UTC")]);
        assert_eq!(rows.len(), 4);
        find_csv_row(
            &rows,
            &[
                ("latitude", "53.00000"),
                ("longitude", "14.40000"),
                ("dateTime", "2024-06-21T18:00:00+00:00"),
            ],
        );
    }
}
