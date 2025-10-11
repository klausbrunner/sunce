mod common;
use assert_cmd::Command;
use common::SunceTest;
use predicates::prelude::*;
use std::io::Write;
use std::process::{Command as StdCommand, Stdio};
use std::time::Duration;

#[test]
fn test_large_coordinate_range_memory_usage() {
    // Test that large coordinate ranges don't cause excessive memory usage
    // This should stream results rather than collecting them all
    let mut cmd = Command::cargo_bin("sunce").unwrap();

    // 101x101 grid = 10,201 points (ranges are inclusive)
    cmd.args([
        "--format=csv",
        "--no-headers",
        "50:60:0.1",
        "10:20:0.1",
        "2024-01-01T12:00:00",
        "position",
    ])
    .timeout(Duration::from_secs(30));

    let output = cmd.output().expect("Failed to execute command");

    assert!(output.status.success(), "Command should succeed");

    // Count lines to verify we got the expected number of results
    let lines: Vec<_> = output
        .stdout
        .split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
        .collect();
    assert_eq!(lines.len(), 10201, "Should produce 101x101 grid points"); // 101 because inclusive range
}

#[test]
fn test_very_fine_coordinate_step() {
    // Test extremely fine step sizes
    let mut cmd = Command::cargo_bin("sunce").unwrap();

    // Very fine step: 0.001 degrees
    cmd.args([
        "--format=csv",
        "--no-headers",
        "52.000:52.010:0.001",
        "13.400",
        "2024-01-01T12:00:00",
        "position",
    ])
    .timeout(Duration::from_secs(5));

    let output = cmd.output().expect("Failed to execute command");

    assert!(output.status.success(), "Command should succeed");

    // Count CSV lines (should be 11 data points: 52.000 to 52.010 inclusive with 0.001 step)
    let lines: Vec<_> = output
        .stdout
        .split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
        .collect();
    assert_eq!(
        lines.len(),
        11,
        "Should produce 11 points with 0.001 step (52.000 to 52.010 inclusive)"
    );
}

#[test]
fn test_year_long_time_series_memory() {
    // Test that a year-long time series with hourly steps doesn't cause memory issues
    let mut cmd = Command::cargo_bin("sunce").unwrap();

    // Full year with hourly steps = 8760 points (or 8784 in leap year)
    cmd.args([
        "--format=csv",
        "--no-headers",
        "52.0",
        "13.4",
        "2024",
        "position",
        "--step=1h",
    ])
    .timeout(Duration::from_secs(30));

    let output = cmd.output().expect("Failed to execute command");

    assert!(output.status.success(), "Command should succeed");

    // 2024 is a leap year: 366 days * 24 hours = 8784 points
    let line_count = output.stdout.iter().filter(|&&b| b == b'\n').count();
    assert!(
        line_count >= 8784,
        "Should produce at least 8784 hourly points for leap year"
    );
}

#[test]
fn test_streaming_with_head_command() {
    // Test that output streams properly and can be interrupted with head
    let mut child = StdCommand::new("cargo")
        .args([
            "run",
            "--",
            "--format=csv",
            "--no-headers",
            "50:90:0.01",
            "10:50:0.01",
            "2024",
            "position",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to spawn sunce");

    // Use head to take only first 10 lines
    let head = StdCommand::new("head")
        .args(["-n", "10"])
        .stdin(child.stdout.take().unwrap())
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to spawn head");

    let head_output = head.wait_with_output().expect("Failed to wait for head");

    // The sunce process should be terminated by SIGPIPE
    // Give it a moment to clean up
    std::thread::sleep(Duration::from_millis(100));

    // Try to kill the child process if it's still running
    let _ = child.kill();
    let _ = child.wait();

    assert!(head_output.status.success(), "Head command should succeed");

    let line_count = head_output.stdout.iter().filter(|&&b| b == b'\n').count();
    assert_eq!(line_count, 10, "Should get exactly 10 lines from head");
}

#[test]
fn test_stdin_streaming_paired_data() {
    // Test streaming paired data through stdin
    let mut child = StdCommand::new("cargo")
        .args([
            "run",
            "--",
            "--format=csv",
            "--no-headers",
            "@-",
            "position",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to spawn sunce");

    let mut stdin = child.stdin.take().expect("Failed to get stdin");

    // Write test data progressively
    let test_data = vec![
        "52.0,13.4,2024-01-01T12:00:00",
        "52.5,13.5,2024-01-02T12:00:00",
        "53.0,14.0,2024-01-03T12:00:00",
    ];

    for line in test_data {
        writeln!(stdin, "{}", line).expect("Failed to write to stdin");
    }

    // Close stdin to signal EOF
    drop(stdin);

    let output = child.wait_with_output().expect("Failed to wait for output");

    assert!(output.status.success(), "Command should succeed");

    let line_count = output.stdout.iter().filter(|&&b| b == b'\n').count();
    assert_eq!(line_count, 3, "Should process 3 input lines");
}

#[test]
fn test_partial_line_handling_in_file() {
    // Test handling of files without final newline
    use tempfile::NamedTempFile;

    let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");

    // Write data without final newline
    write!(
        temp_file,
        "52.0,13.4,2024-01-01T12:00:00\n52.5,13.5,2024-01-02T12:00:00"
    )
    .expect("Failed to write test data");
    temp_file.flush().expect("Failed to flush");

    let mut cmd = Command::cargo_bin("sunce").unwrap();
    cmd.args([
        "--format=csv",
        "--no-headers",
        &format!("@{}", temp_file.path().display()),
        "position",
    ]);

    let output = cmd.output().expect("Failed to execute command");

    assert!(
        output.status.success(),
        "Should handle file without final newline"
    );

    let line_count = output.stdout.iter().filter(|&&b| b == b'\n').count();
    assert_eq!(
        line_count, 2,
        "Should process both lines even without final newline"
    );
}

#[test]
fn test_sigpipe_handling() {
    // Test that SIGPIPE is handled gracefully
    let mut child = StdCommand::new("cargo")
        .args([
            "run",
            "--",
            "--format=csv",
            "50:90:0.1",
            "10:50:0.1",
            "2024",
            "position",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn sunce");

    // Read only a tiny bit of output then close the pipe
    if let Some(mut stdout) = child.stdout.take() {
        let mut buffer = [0; 100];
        let _ = std::io::Read::read(&mut stdout, &mut buffer);
        // Explicitly close stdout by dropping it
        drop(stdout);
    }

    // Give the process a moment to receive SIGPIPE
    std::thread::sleep(Duration::from_millis(100));

    // The process should exit cleanly (killed by SIGPIPE)
    match child.try_wait() {
        Ok(Some(_status)) => {
            // Process has exited - this is expected
            // On Unix, SIGPIPE typically results in exit code 141 (128 + 13)
            // But we accept any termination as success for this test
        }
        Ok(None) => {
            // Process still running - kill it and fail the test
            child.kill().expect("Failed to kill child");
            child.wait().expect("Failed to wait for child");
            panic!("Process didn't handle SIGPIPE properly");
        }
        Err(e) => panic!("Error checking process status: {}", e),
    }
}

#[test]
fn test_coordinate_range_with_negative_values() {
    // Test coordinate ranges that cross zero
    let mut cmd = Command::cargo_bin("sunce").unwrap();

    cmd.args([
        "--format=csv",
        "--no-headers",
        "-5:5:2.5",
        "-10:10:5",
        "2024-06-21T12:00:00",
        "position",
    ]);

    let output = cmd.output().expect("Failed to execute command");

    assert!(
        output.status.success(),
        "Should handle negative coordinate ranges"
    );

    // Expected: latitude -5, -2.5, 0, 2.5, 5 (5 values)
    //           longitude -10, -5, 0, 5, 10 (5 values)
    // Total: 5 * 5 = 25 combinations
    let line_count = output.stdout.iter().filter(|&&b| b == b'\n').count();
    assert_eq!(line_count, 25, "Should produce 5x5 grid");
}

#[test]
fn test_time_series_crossing_dst_boundary() {
    // Test time series that crosses DST transition
    let mut cmd = Command::cargo_bin("sunce").unwrap();

    // March 31, 2024 - DST transition in Europe/Berlin happens at 2:00 AM
    // Use a partial date to generate time series
    cmd.args([
        "--timezone=Europe/Berlin",
        "--format=csv",
        "--show-inputs",
        "52.0",
        "13.4",
        "2024-03-31",
        "position",
        "--step=30m",
    ]);

    let output = cmd.output().expect("Failed to execute command");

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("Command failed: {}", stderr);
    }

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Check that the output contains the DST transition
    // At 2:00 AM, clocks jump to 3:00 AM
    // The exact format may vary depending on if it's T00:30:00 or just T00:30
    let has_130_cet = stdout.contains("T01:30:00+01:00") || stdout.contains("01:30+01:00");
    let has_300_cest = stdout.contains("T03:00:00+02:00") || stdout.contains("03:00+02:00");

    assert!(
        has_130_cet,
        "Should have 1:30 AM CET in output:\n{}",
        stdout
    );
    assert!(
        has_300_cest,
        "Should jump to 3:00 AM CEST in output:\n{}",
        stdout
    );
    assert!(
        !stdout.contains("02:00:00") && !stdout.contains("02:30:00"),
        "Should not have 2:00 AM or 2:30 AM"
    );
}

#[test]
fn test_extreme_latitude_values() {
    // Test calculations at extreme latitudes
    SunceTest::new()
        .args(["89.9", "0", "2024-06-21", "position"])
        .assert_success()
        .stdout(predicate::str::contains("Azimuth"));

    SunceTest::new()
        .args(["-89.9", "0", "2024-12-21", "position"])
        .assert_success()
        .stdout(predicate::str::contains("Azimuth"));

    // Test polar day/night
    SunceTest::new()
        .args(["89.9", "0", "2024-06-21", "sunrise"])
        .assert_success();
}

#[test]
fn test_mixed_input_formats_error_handling() {
    // Test error handling for mixed input formats
    use tempfile::NamedTempFile;

    let mut coords_file = NamedTempFile::new().expect("Failed to create temp file");
    writeln!(coords_file, "52.0,13.4").expect("Failed to write");
    coords_file.flush().expect("Failed to flush");

    let mut times_file = NamedTempFile::new().expect("Failed to create temp file");
    writeln!(times_file, "2024-01-01").expect("Failed to write");
    times_file.flush().expect("Failed to flush");

    // This should work: coordinate file + time file
    SunceTest::new()
        .args([
            &format!("@{}", coords_file.path().display()),
            &format!("@{}", times_file.path().display()),
            "position",
        ])
        .assert_success();

    // This should fail: paired file doesn't work with separate time file
    let mut paired_file = NamedTempFile::new().expect("Failed to create temp file");
    writeln!(paired_file, "52.0,13.4,2024-01-01").expect("Failed to write");
    paired_file.flush().expect("Failed to flush");

    SunceTest::new()
        .args([
            &format!("@{}", paired_file.path().display()),
            &format!("@{}", times_file.path().display()),
            "position",
        ])
        .assert_failure();
}

#[test]
fn test_empty_file_handling() {
    use tempfile::NamedTempFile;

    let empty_file = NamedTempFile::new().expect("Failed to create temp file");

    // Empty files should succeed but produce no output
    SunceTest::new()
        .args([&format!("@{}", empty_file.path().display()), "position"])
        .assert_success()
        .stdout(predicate::str::is_empty());
}

#[test]
fn test_unicode_in_error_messages() {
    // Test that unicode in file paths is handled correctly
    use std::fs;
    use tempfile::tempdir;

    let dir = tempdir().expect("Failed to create temp dir");
    let unicode_path = dir.path().join("файл_文件.txt");
    fs::write(&unicode_path, "invalid data").expect("Failed to write file");

    SunceTest::new()
        .args([&format!("@{}", unicode_path.display()), "position"])
        .assert_failure();
}

#[test]
fn test_negative_coordinates_position() {
    // Sydney, Australia (negative latitude)
    SunceTest::new()
        .args([
            "--format=csv",
            "--no-headers",
            "-33.8688",
            "151.2093",
            "2024-06-21T12:00:00",
            "position",
        ])
        .assert_success();

    // Buenos Aires, Argentina (negative latitude and longitude)
    SunceTest::new()
        .args([
            "--format=csv",
            "--no-headers",
            "-34.6037",
            "-58.3816",
            "2024-12-21T12:00:00",
            "position",
        ])
        .assert_success();

    // West of Prime Meridian (negative longitude only)
    SunceTest::new()
        .args([
            "--format=csv",
            "--no-headers",
            "40.7128",
            "-74.0060",
            "2024-03-20T12:00:00",
            "position",
        ])
        .assert_success();
}

#[test]
fn test_negative_coordinates_sunrise() {
    // Sydney, Australia - should have winter sunrise/sunset
    SunceTest::new()
        .args([
            "--format=csv",
            "--no-headers",
            "-33.8688",
            "151.2093",
            "2024-06-21",
            "sunrise",
        ])
        .assert_success();

    // Buenos Aires, Argentina - should have summer sunrise/sunset
    SunceTest::new()
        .args([
            "--format=csv",
            "--no-headers",
            "-34.6037",
            "-58.3816",
            "2024-12-21",
            "sunrise",
        ])
        .assert_success();

    // Cape Town, South Africa (negative latitude)
    SunceTest::new()
        .args([
            "--format=csv",
            "--no-headers",
            "-33.9249",
            "18.4241",
            "2024-01-15",
            "sunrise",
        ])
        .assert_success();
}

#[test]
fn test_negative_longitude_range() {
    // Test longitude range from positive to negative (crossing Prime Meridian)
    let mut cmd = Command::cargo_bin("sunce").unwrap();

    cmd.args([
        "--format=csv",
        "--no-headers",
        "51.5",
        "-5:5:2.5",
        "2024-06-21T12:00:00",
        "position",
    ]);

    let output = cmd.output().expect("Failed to execute command");
    assert!(
        output.status.success(),
        "Should handle longitude range crossing Prime Meridian"
    );

    // Expected: longitude -5, -2.5, 0, 2.5, 5 (5 values)
    let line_count = output.stdout.iter().filter(|&&b| b == b'\n').count();
    assert_eq!(line_count, 5, "Should produce 5 longitude points");
}

#[test]
fn test_negative_latitude_range_southern_hemisphere() {
    // Test latitude range entirely in southern hemisphere
    let mut cmd = Command::cargo_bin("sunce").unwrap();

    cmd.args([
        "--format=csv",
        "--no-headers",
        "-40:-30:5",
        "150",
        "2024-12-21T12:00:00",
        "position",
    ]);

    let output = cmd.output().expect("Failed to execute command");
    assert!(
        output.status.success(),
        "Should handle negative latitude range"
    );

    // Expected: latitude -40, -35, -30 (3 values)
    let line_count = output.stdout.iter().filter(|&&b| b == b'\n').count();
    assert_eq!(line_count, 3, "Should produce 3 latitude points");
}

#[test]
fn test_both_coordinate_ranges_crossing_zero() {
    // Test both coordinates crossing zero simultaneously
    let mut cmd = Command::cargo_bin("sunce").unwrap();

    cmd.args([
        "--format=csv",
        "--no-headers",
        "-2:2:2",
        "-2:2:2",
        "2024-06-21T12:00:00",
        "position",
    ]);

    let output = cmd.output().expect("Failed to execute command");
    assert!(
        output.status.success(),
        "Should handle both coordinates crossing zero"
    );

    // Expected: lat -2, 0, 2 (3 values) × lon -2, 0, 2 (3 values) = 9 combinations
    let line_count = output.stdout.iter().filter(|&&b| b == b'\n').count();
    assert_eq!(line_count, 9, "Should produce 3x3 grid");
}

#[test]
fn test_extreme_negative_coordinates() {
    // Test near south pole
    SunceTest::new()
        .args([
            "--format=csv",
            "-89.5",
            "0",
            "2024-12-21T12:00:00",
            "position",
        ])
        .assert_success();

    // Test date line crossing with negative coordinates
    SunceTest::new()
        .args([
            "--format=csv",
            "-45.0",
            "-179.9",
            "2024-06-21T12:00:00",
            "position",
        ])
        .assert_success();
}
