use sunce::input;

#[test]
fn calendar_periods_cover_leap_years_and_year_boundaries() {
    for (period, days, first, last) in [
        ("2024", 366, "2024-01-01", "2024-12-31"),
        ("2024-02", 29, "2024-02-01", "2024-02-29"),
        ("2025-02", 28, "2025-02-01", "2025-02-28"),
        ("2026-12", 31, "2026-12-01", "2026-12-31"),
        ("2026-12-31/2027-01-01", 2, "2026-12-31", "2027-01-01"),
    ] {
        let times = input::position_times(period, "UTC".parse().unwrap(), "1d".parse().unwrap())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(times.len(), days);
        assert_eq!(times[0].date_naive().to_string(), first);
        assert_eq!(times.last().unwrap().date_naive().to_string(), last);
    }
    for period in ["2026-00", "2026-13", "2025-02-29", "2026-01-01/2026-02-30"] {
        assert!(
            input::position_times(period, "UTC".parse().unwrap(), "1d".parse().unwrap()).is_err()
        );
    }
}

#[test]
fn inclusive_date_ranges_sample_elapsed_time_across_dst() {
    let zone = "Europe/Berlin".parse().unwrap();
    let times = input::position_times("2026-03-28/2026-03-30", zone, "1h".parse().unwrap())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(times.len(), 71);
    assert_eq!(times[0].to_rfc3339(), "2026-03-28T00:00:00+01:00");
    assert_eq!(
        times.last().unwrap().to_rfc3339(),
        "2026-03-30T23:00:00+02:00"
    );
    assert!(
        times
            .windows(2)
            .all(|pair| pair[1].timestamp() - pair[0].timestamp() == 3600)
    );
    for period in [
        "2026-03-30/2026-03-28",
        "2026-02-30/2026-03-01",
        "2026/2027",
    ] {
        assert!(input::position_times(period, zone, "1h".parse().unwrap()).is_err());
    }
}

#[test]
fn daily_samples_keep_local_time_and_resolve_dst_gaps_and_overlaps() {
    let zone = "Europe/Berlin".parse().unwrap();
    let collect = |period, clock: &str| {
        input::daily_times(period, zone, clock.parse().unwrap())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    let noon = collect("2026-03-28/2026-03-30", "12:34:56");
    assert_eq!(noon.len(), 3);
    assert!(
        noon.iter()
            .all(|time| time.time().to_string() == "12:34:56")
    );
    assert_eq!(noon[1].timestamp() - noon[0].timestamp(), 23 * 3600);
    assert_eq!(
        collect("2026-03-29", "02:30:00")[0].to_rfc3339(),
        "2026-03-29T03:00:00+02:00"
    );
    assert_eq!(
        collect("2026-10-25", "02:30:00")[0].to_rfc3339(),
        "2026-10-25T02:30:00+02:00"
    );
    let skipped = input::daily_times(
        "2011-12-29/2011-12-31",
        "Pacific/Apia".parse().unwrap(),
        "12:00:00".parse().unwrap(),
    )
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    assert_eq!(skipped.len(), 2);
    assert_eq!(skipped[1].date_naive().to_string(), "2011-12-31");
}

#[cfg(feature = "cli")]
#[test]
fn cli_date_ranges_and_daily_clock_sampling_match_the_library() {
    let output = |args: &[&str]| {
        std::process::Command::new(env!("CARGO_BIN_EXE_sunce"))
            .args(args)
            .output()
            .unwrap()
    };
    let result = output(&[
        "--timezone=Europe/Berlin",
        "--format=csv",
        "52.52",
        "13.405",
        "2026-03-28/2026-03-30",
        "position",
        "--at=12:34:56",
    ]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let csv = String::from_utf8(result.stdout).unwrap();
    assert_eq!(csv.lines().count(), 4);
    assert!(csv.contains("2026-03-29T12:34:56+02:00"));
    let events = output(&[
        "--timezone=Europe/Berlin",
        "--format=csv",
        "52.52",
        "13.405",
        "2026-03-28/2026-03-30",
        "events",
    ]);
    assert!(events.status.success());
    assert!(
        String::from_utf8(events.stdout)
            .unwrap()
            .contains("2026-03-30")
    );
    for args in [
        vec![
            "0",
            "0",
            "2026-01",
            "position",
            "--at=12:00:00",
            "--step=1h",
        ],
        vec!["0", "0", "2026-01", "events", "--at=12:00:00"],
        vec!["0", "0", "now", "position", "--at=12:00:00"],
        vec!["0", "0", "2026-01", "position", "--at=25:00:00"],
        vec!["0", "0", "2026-01-01T12:00:00", "position", "--at=12:00:00"],
        vec!["0", "0", "2026-02-01/2026-01-01", "events"],
    ] {
        assert!(!output(&args).status.success(), "{args:?}");
    }
}
