#![allow(dead_code)]

use assert_cmd::Command;
use chrono::{DateTime, FixedOffset};
use csv::ReaderBuilder;
use predicates::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Test helper for running sunce commands with less boilerplate
pub struct SunceTest {
    cmd: Command,
}

pub fn sunce_exe_path() -> PathBuf {
    let exe = assert_cmd::cargo::cargo_bin!("sunce");
    if exe.is_absolute() {
        exe.to_path_buf()
    } else {
        Path::new(env!("CARGO_MANIFEST_DIR")).join(exe)
    }
}

pub fn sunce_command() -> Command {
    Command::new(sunce_exe_path())
}

impl SunceTest {
    /// Create a new sunce command test
    pub fn new() -> Self {
        Self {
            cmd: sunce_command(),
        }
    }

    /// Add arguments to the command
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        self.cmd.args(args);
        self
    }

    /// Add a single argument to the command
    pub fn arg<S: AsRef<std::ffi::OsStr>>(mut self, arg: S) -> Self {
        self.cmd.arg(arg);
        self
    }

    /// Assert the command succeeds
    pub fn assert_success(mut self) -> assert_cmd::assert::Assert {
        self.cmd.assert().success()
    }

    /// Assert the command succeeds and contains text in stdout
    pub fn assert_success_contains(mut self, text: &str) -> assert_cmd::assert::Assert {
        self.cmd
            .assert()
            .success()
            .stdout(predicate::str::contains(text))
    }

    /// Assert the command succeeds and contains all texts in stdout
    pub fn assert_success_contains_all(mut self, texts: &[&str]) -> assert_cmd::assert::Assert {
        let mut assertion = self.cmd.assert().success();
        for text in texts {
            assertion = assertion.stdout(predicate::str::contains(*text));
        }
        assertion
    }

    /// Assert the command fails
    pub fn assert_failure(mut self) -> assert_cmd::assert::Assert {
        self.cmd.assert().failure()
    }

    /// Get the raw command for complex assertions (when helpers aren't enough)
    pub fn command(self) -> Command {
        self.cmd
    }

    /// Get command output for inspection
    pub fn get_output(mut self) -> std::process::Output {
        self.cmd.output().unwrap()
    }
}

/// Quick helper for position calculations
pub fn position_test() -> SunceTest {
    SunceTest::new().args(["52.0", "13.4", "2024-01-01T12:00:00", "position"])
}

/// Quick helper for position calculations with global options (put before positional args)
pub fn position_test_with_format(format: &str) -> SunceTest {
    SunceTest::new().args([
        &format!("--format={}", format),
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
    ])
}

/// Quick helper for custom coordinates
pub fn custom_position(lat: &str, lon: &str, datetime: &str) -> SunceTest {
    SunceTest::new().args([lat, lon, datetime, "position"])
}

/// Quick helper for position with elevation angle
pub fn position_test_with_elevation() -> SunceTest {
    SunceTest::new().args([
        "--format=CSV",
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
        "--elevation-angle",
    ])
}

/// Quick helper for coordinate ranges test
pub fn coordinate_range_test() -> SunceTest {
    SunceTest::new().args([
        "--format=CSV",
        "52:53:1",
        "13:14:1",
        "2024-01-01T12:00:00",
        "position",
    ])
}

/// Quick helper for time series with step
pub fn time_series_test(date: &str, step: &str) -> SunceTest {
    SunceTest::new().args([
        "--format=CSV",
        "52.0",
        "13.4",
        date,
        "position",
        &format!("--step={}", step),
    ])
}

/// Quick helper for show-inputs test (latitude range)
pub fn show_inputs_lat_range_test() -> SunceTest {
    SunceTest::new().args([
        "--format=CSV",
        "52:53:1",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
    ])
}

/// Quick helper for show-inputs test with explicit disable
pub fn show_inputs_disabled_test() -> SunceTest {
    SunceTest::new().args([
        "--format=CSV",
        "--no-show-inputs",
        "52:53:1",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
    ])
}

/// Quick helper for environmental parameters test
pub fn environmental_params_test() -> SunceTest {
    SunceTest::new().args([
        "--format=CSV",
        "--show-inputs",
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
        "--elevation=1000",
        "--pressure=900",
        "--temperature=25",
    ])
}

/// Quick helper for position with no refraction
pub fn position_no_refraction_test() -> SunceTest {
    SunceTest::new().args([
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
        "--no-refraction",
    ])
}

/// Quick helper for position with timezone
pub fn position_with_timezone(tz: &str) -> SunceTest {
    SunceTest::new().args([
        &format!("--timezone={}", tz),
        "--show-inputs",
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
    ])
}

/// Quick helper for combined range and time series test
pub fn combined_range_time_test() -> SunceTest {
    SunceTest::new().args([
        "--format=CSV",
        "52:53:1",
        "13:14:1",
        "2024-01-01",
        "position",
        "--step=12h",
    ])
}

/// Quick helper for CSV output without headers
pub fn position_csv_no_headers() -> SunceTest {
    SunceTest::new().args([
        "--format=CSV",
        "--no-headers",
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
    ])
}

/// Quick helper for delta T test
pub fn position_with_deltat(deltat: &str) -> SunceTest {
    SunceTest::new().args([
        "--format=CSV",
        "--show-inputs",
        &format!("--deltat={}", deltat),
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
    ])
}

/// Quick helper for delta T estimation
pub fn position_with_deltat_estimation() -> SunceTest {
    SunceTest::new().args([
        "--deltat",
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
    ])
}

/// Quick helper for missing arguments test
pub fn missing_args_test() -> SunceTest {
    SunceTest::new().args(["52.0"])
}

/// Quick helper for invalid step test
pub fn invalid_step_test() -> SunceTest {
    SunceTest::new().args([
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
        "--step=invalid",
    ])
}

/// Quick helper for invalid algorithm test
pub fn invalid_algorithm_test() -> SunceTest {
    SunceTest::new().args([
        "52.0",
        "13.4",
        "2024-01-01T12:00:00",
        "position",
        "--algorithm=INVALID",
    ])
}

/// Parse command stdout as JSON.
pub fn parse_json_output(stdout: &str) -> Value {
    serde_json::from_str(stdout).expect("invalid JSON output")
}

pub fn fields(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_string()).collect()
}

/// Parse CSV output into header and records.
pub fn parse_csv_output(stdout: &str) -> (Vec<String>, Vec<Vec<String>>) {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(stdout.as_bytes());
    let headers = reader
        .headers()
        .expect("CSV header line missing")
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>();
    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record.expect("invalid CSV record");
        let row = record
            .iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>();
        assert_eq!(row.len(), headers.len(), "CSV row width mismatch");
        rows.push(row);
    }

    (headers, rows)
}

/// Parse CSV output that has no header row.
pub fn parse_csv_no_headers_output(stdout: &str) -> Vec<Vec<String>> {
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .from_reader(stdout.as_bytes());
    reader
        .records()
        .map(|r| {
            r.expect("invalid CSV record")
                .iter()
                .map(std::string::ToString::to_string)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
}

/// Parse CSV output that must contain exactly one record.
pub fn parse_csv_single_record(stdout: &str) -> (Vec<String>, Vec<String>) {
    let (headers, rows) = parse_csv_output(stdout);
    assert_eq!(rows.len(), 1, "expected exactly one CSV record");
    (headers, rows[0].clone())
}

pub fn parse_csv_single_record_map(stdout: &str) -> HashMap<String, String> {
    let (headers, row) = parse_csv_single_record(stdout);
    csv_row_map(&headers, &row)
}

pub fn parse_csv_output_maps(stdout: &str) -> Vec<HashMap<String, String>> {
    let (headers, rows) = parse_csv_output(stdout);
    rows.iter().map(|row| csv_row_map(&headers, row)).collect()
}

/// Convert a CSV row into a field map keyed by header name.
pub fn csv_row_map(headers: &[String], row: &[String]) -> HashMap<String, String> {
    assert_eq!(
        headers.len(),
        row.len(),
        "CSV header/value column count mismatch"
    );
    headers
        .iter()
        .cloned()
        .zip(row.iter().cloned())
        .collect::<HashMap<_, _>>()
}

pub fn find_csv_row<'a>(
    rows: &'a [HashMap<String, String>],
    filters: &[(&str, &str)],
) -> &'a HashMap<String, String> {
    rows.iter()
        .find(|row| {
            filters
                .iter()
                .all(|(field, expected)| row.get(*field).map(String::as_str) == Some(*expected))
        })
        .unwrap_or_else(|| panic!("missing CSV row matching filters: {filters:?}"))
}

pub fn load_fixture_rows(name: &str) -> Vec<HashMap<String, String>> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name);
    let text = fs::read_to_string(&path).unwrap_or_else(|err| {
        panic!("failed to read fixture {}: {}", path.display(), err);
    });
    parse_csv_output_maps(&text)
}

pub fn write_text_file(path: &Path, contents: &str) {
    fs::write(path, contents).unwrap_or_else(|err| {
        panic!("failed to write {}: {}", path.display(), err);
    });
}

/// Parse RFC3339 timestamp.
pub fn parse_rfc3339(ts: &str) -> DateTime<FixedOffset> {
    DateTime::parse_from_rfc3339(ts).expect("invalid RFC3339 datetime")
}

/// Assert two RFC3339 timestamps are within tolerance seconds.
pub fn assert_time_close(actual: &str, expected: &str, tolerance_seconds: i64) {
    let actual = parse_rfc3339(actual);
    let expected = parse_rfc3339(expected);
    let delta = (actual.timestamp() - expected.timestamp()).abs();
    assert!(
        delta <= tolerance_seconds,
        "time mismatch: actual={actual}, expected={expected}, tolerance={tolerance_seconds}s"
    );
}
