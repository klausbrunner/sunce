mod common;
use common::{SunceTest, parse_csv_output_maps, sunce_command, sunce_exe_path, write_text_file};
use predicates::prelude::*;
use std::io::{Read, Write};
use std::process::{Command as StdCommand, Stdio};
use std::thread;
use std::time::Duration;
use tempfile::NamedTempFile;

fn timed_output(args: &[&str], timeout: Duration) -> std::process::Output {
    sunce_command()
        .args(args)
        .timeout(timeout)
        .output()
        .expect("Failed to execute command")
}

fn no_header_line_count(args: &[&str], timeout: Duration) -> usize {
    let output = timed_output(args, timeout);
    assert!(
        output.status.success(),
        "command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout.iter().filter(|&&b| b == b'\n').count()
}

#[test]
fn test_large_outputs_and_ranges() {
    for (args, timeout, expected) in [
        (
            vec![
                "--format=csv",
                "--no-headers",
                "50:60:0.1",
                "10:20:0.1",
                "2024-01-01T12:00:00",
                "position",
            ],
            Duration::from_secs(30),
            10201,
        ),
        (
            vec![
                "--format=csv",
                "--no-headers",
                "52.000:52.010:0.001",
                "13.400",
                "2024-01-01T12:00:00",
                "position",
            ],
            Duration::from_secs(5),
            11,
        ),
        (
            vec![
                "--format=csv",
                "--no-headers",
                "-5:5:2.5",
                "-10:10:5",
                "2024-06-21T12:00:00",
                "position",
            ],
            Duration::from_secs(5),
            25,
        ),
        (
            vec![
                "--format=csv",
                "--no-headers",
                "51.5",
                "-5:5:2.5",
                "2024-06-21T12:00:00",
                "position",
            ],
            Duration::from_secs(5),
            5,
        ),
        (
            vec![
                "--format=csv",
                "--no-headers",
                "-40:-30:5",
                "150",
                "2024-12-21T12:00:00",
                "position",
            ],
            Duration::from_secs(5),
            3,
        ),
        (
            vec![
                "--format=csv",
                "--no-headers",
                "-2:2:2",
                "-2:2:2",
                "2024-06-21T12:00:00",
                "position",
            ],
            Duration::from_secs(5),
            9,
        ),
    ] {
        assert_eq!(no_header_line_count(&args, timeout), expected);
    }

    let year_output = timed_output(
        &[
            "--format=csv",
            "--no-headers",
            "52.0",
            "13.4",
            "2024",
            "position",
            "--step=1h",
        ],
        Duration::from_secs(30),
    );
    assert!(year_output.status.success());
    assert!(year_output.stdout.iter().filter(|&&b| b == b'\n').count() >= 8784);
}

#[test]
fn test_unbounded_watch_requires_single_location() {
    sunce_command()
        .args(["52:53:1", "13.4", "now", "--step=1m", "position"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Cannot use an unbounded time stream",
        ));
}

#[test]
fn test_streaming_with_head_command() {
    let mut child = StdCommand::new(sunce_exe_path())
        .args([
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

    let head = StdCommand::new("head")
        .args(["-n", "10"])
        .stdin(child.stdout.take().unwrap())
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to spawn head");

    let head_output = head.wait_with_output().expect("Failed to wait for head");
    thread::sleep(Duration::from_millis(100));
    let _ = child.kill();
    let _ = child.wait();

    assert!(head_output.status.success());
    assert_eq!(
        head_output.stdout.iter().filter(|&&b| b == b'\n').count(),
        10
    );
}

#[test]
fn test_stdin_streaming_paired_data() {
    let mut child = StdCommand::new(sunce_exe_path())
        .args(["--format=csv", "--no-headers", "@-", "position"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to spawn sunce");

    let mut stdin = child.stdin.take().expect("Failed to get stdin");
    for line in [
        "52.0,13.4,2024-01-01T12:00:00",
        "52.5,13.5,2024-01-02T12:00:00",
        "53.0,14.0,2024-01-03T12:00:00",
    ] {
        writeln!(stdin, "{line}").expect("Failed to write to stdin");
    }
    drop(stdin);

    let output = child.wait_with_output().expect("Failed to wait for output");
    assert!(output.status.success());
    assert_eq!(output.stdout.iter().filter(|&&b| b == b'\n').count(), 3);
}

#[test]
fn test_partial_line_handling_in_file() {
    let mut file = NamedTempFile::new().expect("Failed to create temp file");
    write!(
        file,
        "52.0,13.4,2024-01-01T12:00:00\n52.5,13.5,2024-01-02T12:00:00"
    )
    .expect("Failed to write test data");
    file.flush().expect("Failed to flush");

    let output = timed_output(
        &[
            "--format=csv",
            "--no-headers",
            &format!("@{}", file.path().display()),
            "position",
        ],
        Duration::from_secs(5),
    );
    assert!(output.status.success());
    assert_eq!(output.stdout.iter().filter(|&&b| b == b'\n').count(), 2);
}

#[test]
fn test_sigpipe_handling() {
    let mut child = StdCommand::new(sunce_exe_path())
        .args(["--format=csv", "50:90:0.1", "10:50:0.1", "2024", "position"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn sunce");

    if let Some(mut stdout) = child.stdout.take() {
        let mut buffer = [0; 100];
        let _ = stdout.read(&mut buffer);
        drop(stdout);
    }

    thread::sleep(Duration::from_millis(100));
    match child.try_wait() {
        Ok(Some(_)) => {}
        Ok(None) => {
            child.kill().expect("Failed to kill child");
            child.wait().expect("Failed to wait for child");
            panic!("Process didn't handle SIGPIPE properly");
        }
        Err(err) => panic!("Error checking process status: {err}"),
    }
}

#[test]
fn test_time_series_crossing_dst_boundary() {
    let output = timed_output(
        &[
            "--timezone=Europe/Berlin",
            "--format=csv",
            "--show-inputs",
            "52.0",
            "13.4",
            "2024-03-31",
            "position",
            "--step=30m",
        ],
        Duration::from_secs(10),
    );
    assert!(output.status.success());

    let rows = parse_csv_output_maps(&String::from_utf8(output.stdout).unwrap());
    let datetimes = rows
        .iter()
        .map(|row| row["dateTime"].clone())
        .collect::<Vec<_>>();
    assert!(datetimes.iter().any(|ts| ts == "2024-03-31T01:30:00+01:00"));
    assert!(datetimes.iter().any(|ts| ts == "2024-03-31T03:00:00+02:00"));
    assert!(
        datetimes
            .iter()
            .all(|ts| !ts.contains("T02:00:00") && !ts.contains("T02:30:00"))
    );
}

#[test]
fn test_extreme_and_negative_coordinates() {
    for args in [
        vec!["89.9", "0", "2024-06-21", "position"],
        vec!["-89.9", "0", "2024-12-21", "position"],
        vec!["-33.8688", "151.2093", "2024-06-21T12:00:00", "position"],
        vec!["-34.6037", "-58.3816", "2024-12-21T12:00:00", "position"],
        vec!["40.7128", "-74.0060", "2024-03-20T12:00:00", "position"],
        vec!["-89.5", "0", "2024-12-21T12:00:00", "position"],
        vec!["-45.0", "-179.9", "2024-06-21T12:00:00", "position"],
    ] {
        SunceTest::new().args(args).assert_success();
    }

    for args in [
        vec!["89.9", "0", "2024-06-21", "sunrise"],
        vec!["-33.8688", "151.2093", "2024-06-21", "sunrise"],
        vec!["-34.6037", "-58.3816", "2024-12-21", "sunrise"],
        vec!["-33.9249", "18.4241", "2024-01-15", "sunrise"],
    ] {
        SunceTest::new().args(args).assert_success();
    }
}

#[test]
fn test_mixed_input_formats_error_handling() {
    let mut coords_file = NamedTempFile::new().expect("Failed to create temp file");
    writeln!(coords_file, "52.0,13.4").expect("Failed to write");
    coords_file.flush().expect("Failed to flush");

    let mut times_file = NamedTempFile::new().expect("Failed to create temp file");
    writeln!(times_file, "2024-01-01").expect("Failed to write");
    times_file.flush().expect("Failed to flush");

    SunceTest::new()
        .args([
            &format!("@{}", coords_file.path().display()),
            &format!("@{}", times_file.path().display()),
            "position",
        ])
        .assert_success();

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
    let empty_file = NamedTempFile::new().expect("Failed to create temp file");
    SunceTest::new()
        .args([&format!("@{}", empty_file.path().display()), "position"])
        .assert_success()
        .stdout(predicate::str::is_empty());
}

#[test]
fn test_unicode_in_error_messages() {
    let dir = tempfile::tempdir().expect("Failed to create temp dir");
    let unicode_path = dir.path().join("файл_文件.txt");
    write_text_file(&unicode_path, "invalid data");
    SunceTest::new()
        .args([&format!("@{}", unicode_path.display()), "position"])
        .assert_failure();
}
