mod common;
use common::{sunce_command, sunce_exe_path, write_text_file};
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
    assert_eq!(
        no_header_line_count(
            &[
                "--format=csv",
                "--no-headers",
                "50:60:0.1",
                "10:20:0.1",
                "2024-01-01T12:00:00",
                "position",
            ],
            Duration::from_secs(30),
        ),
        10201
    );
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
fn test_extreme_and_negative_coordinates() {
    sunce_command()
        .args(["-45", "-179.9", "2024-06-21T12:00:00", "position"])
        .assert()
        .success();
    sunce_command()
        .args(["89.9", "0", "2024-06-21", "sunrise"])
        .assert()
        .success();
}

#[test]
fn test_mixed_input_formats_error_handling() {
    let mut times_file = NamedTempFile::new().expect("Failed to create temp file");
    writeln!(times_file, "2024-01-01").expect("Failed to write");
    times_file.flush().expect("Failed to flush");

    let mut paired_file = NamedTempFile::new().expect("Failed to create temp file");
    writeln!(paired_file, "52.0,13.4,2024-01-01").expect("Failed to write");
    paired_file.flush().expect("Failed to flush");

    sunce_command()
        .args([
            &format!("@{}", paired_file.path().display()),
            &format!("@{}", times_file.path().display()),
            "position",
        ])
        .assert()
        .failure();
}

#[test]
fn test_empty_file_handling() {
    let empty_file = NamedTempFile::new().expect("Failed to create temp file");
    sunce_command()
        .args([&format!("@{}", empty_file.path().display()), "position"])
        .assert()
        .success()
        .stdout(predicate::str::is_empty());
}

#[test]
fn test_unicode_in_error_messages() {
    let dir = tempfile::tempdir().expect("Failed to create temp dir");
    let unicode_path = dir.path().join("файл_文件.txt");
    write_text_file(&unicode_path, "invalid data");
    sunce_command()
        .args([&format!("@{}", unicode_path.display()), "position"])
        .assert()
        .failure();
}
