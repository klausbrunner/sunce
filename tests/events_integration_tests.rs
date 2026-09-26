mod common;
use common::{parse_csv_output_maps, parse_json_lines, parse_rfc3339, sunce_command};
use std::collections::HashSet;

fn output(args: &[&str], zone: &str) -> String {
    String::from_utf8(
        sunce_command()
            .env("TZ", zone)
            .args(args)
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap()
}

#[test]
fn daily_rows_are_chronological_and_keep_dates_and_coordinates() {
    let rows = parse_csv_output_maps(&output(
        &["--format=csv", "52:53:1", "13:14:1", "2024-06", "events"],
        "Europe/Berlin",
    ));
    assert_eq!(rows.len(), 4 * 30 * 3);
    let mut days = HashSet::new();
    for day in rows.as_chunks::<3>().0 {
        assert_eq!(
            day.iter().map(|r| r["event"].as_str()).collect::<Vec<_>>(),
            ["sunrise", "transit", "sunset"]
        );
        for row in day {
            assert_eq!(row["day_state"], "CROSSING");
            assert!(row["time"].starts_with(&row["date"]));
            assert!(row["time"].ends_with("+02:00"));
        }
        assert!(
            day.windows(2)
                .all(|p| parse_rfc3339(&p[0]["time"]) < parse_rfc3339(&p[1]["time"]))
        );
        assert!(days.insert((
            day[0]["latitude"].clone(),
            day[0]["longitude"].clone(),
            day[0]["date"].clone()
        )));
    }
    assert_eq!(days.len(), 120);
}

#[test]
fn preserves_multiple_missing_and_unpaired_events() {
    for algorithm in ["spa", "grena3"] {
        for (date, lat, lon, rises, transits, sets, state) in [
            ("2020-04-16", "78.216667", "15.633333", 2, 1, 1, "CROSSING"),
            ("2020-08-25", "78.216667", "15.633333", 0, 1, 1, "CROSSING"),
            ("2020-06-10", "0", "179.9", 1, 0, 1, "CROSSING"),
            ("2024-06-21", "90", "0", 0, 1, 0, "ABOVE"),
            ("2024-12-21", "90", "0", 0, 1, 0, "BELOW"),
            ("2020-06-10", "90", "179.9", 0, 0, 0, "ABOVE"),
        ] {
            let rows = parse_json_lines(&output(
                &[
                    "--format=json",
                    "--timezone=UTC",
                    "--deltat=69.184",
                    "--algorithm",
                    algorithm,
                    lat,
                    lon,
                    date,
                    "events",
                ],
                "UTC",
            ));
            assert_eq!(
                rows.len(),
                (rises + transits + sets).max(1),
                "{algorithm} {date}"
            );
            for (event, count) in [("sunrise", rises), ("transit", transits), ("sunset", sets)] {
                assert_eq!(rows.iter().filter(|r| r["event"] == event).count(), count);
            }
            for row in rows {
                assert_eq!(row["date"], date);
                assert_eq!(row["day_state"], state);
                assert_eq!(row["event"].is_null(), row["time"].is_null());
            }
        }
    }
}

#[test]
fn event_times_preserve_named_zone_rules_and_explicit_offsets() {
    for (input, zone, suffix) in [
        ("2024-03-31", "Europe/Berlin", "+02:00"),
        ("2024-10-27", "Europe/Berlin", "+01:00"),
        ("2024-03-31T00:00:00+01:00", "Europe/Berlin", "+01:00"),
        ("2024-10-27T00:00:00+02:00", "Europe/Berlin", "+02:00"),
    ] {
        let rows = parse_csv_output_maps(&output(
            &["--format=csv", "52", "13.4", input, "events"],
            zone,
        ));
        assert_eq!(rows.len(), 3);
        assert!(
            rows.iter().all(|r| r["time"].ends_with(suffix)),
            "{input}: {rows:?}"
        );
    }
    let rows = parse_csv_output_maps(&output(
        &[
            "--format=csv",
            "--timezone=Europe/Berlin",
            "52",
            "13.4",
            "2024-03-31T00:00:00+01:00",
            "events",
        ],
        "UTC",
    ));
    assert!(rows.iter().all(|r| r["time"].ends_with("+02:00")));
}
