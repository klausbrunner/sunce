#![allow(dead_code)]

use assert_cmd::Command;
use predicates::prelude::*;

/// Test helper for running sunce commands with less boilerplate
pub struct SunceTest {
    cmd: Command,
}

pub fn sunce_command() -> Command {
    Command::new(assert_cmd::cargo::cargo_bin!("sunce"))
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
