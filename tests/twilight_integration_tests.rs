mod common;
use chrono::{NaiveDate, Utc};
use common::*;
use solar_positioning::{Horizon, Location, SolarEvents};

#[test]
fn twilight_rows_match_library_and_json_matches_csv() {
    let args = [
        "--timezone=UTC",
        "--deltat=69.184",
        "52",
        "13.4",
        "2024-06-21",
        "events",
        "--twilight",
    ];
    let csv = sunce_command()
        .args(args)
        .arg("--format=csv")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let json = sunce_command()
        .args(args)
        .arg("--format=json")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let csv = parse_csv_output_maps(std::str::from_utf8(&csv).unwrap());
    let json = parse_json_lines(std::str::from_utf8(&json).unwrap());
    assert_eq!(csv.len(), json.len());
    for (csv, json) in csv.iter().zip(json) {
        for (name, value) in csv {
            assert_eq!(json[name].as_str(), Some(value.as_str()));
        }
    }

    let mut expected = Vec::new();
    for (horizon, rise, set) in [
        (Horizon::SunriseSunset, "sunrise", "sunset"),
        (Horizon::CivilTwilight, "civil_dawn", "civil_dusk"),
        (Horizon::NauticalTwilight, "nautical_dawn", "nautical_dusk"),
        (
            Horizon::AstronomicalTwilight,
            "astronomical_dawn",
            "astronomical_dusk",
        ),
    ] {
        let day = SolarEvents::new()
            .for_date(
                NaiveDate::from_ymd_opt(2024, 6, 21).unwrap(),
                &Utc,
                Location {
                    latitude: 52.0,
                    longitude: 13.4,
                },
                69.184,
                horizon,
            )
            .unwrap();
        for (name, times) in [(rise, day.rises), (set, day.sets)] {
            expected.extend(times.into_iter().map(|t| (name, t.timestamp())));
        }
        if horizon == Horizon::SunriseSunset {
            expected.extend(day.transits.into_iter().map(|t| ("transit", t.timestamp())));
        }
    }
    expected.sort_by_key(|(_, time)| *time);
    let actual = csv
        .iter()
        .map(|r| (r["event"].as_str(), parse_rfc3339(&r["time"]).timestamp()))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

#[test]
fn custom_horizon_uses_rise_and_set_labels() {
    let output = sunce_command()
        .args([
            "--timezone=UTC",
            "--format=csv",
            "52",
            "13.4",
            "2024-03-20",
            "events",
            "--horizon=-4.5",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let rows = parse_csv_output_maps(std::str::from_utf8(&output).unwrap());
    assert_eq!(
        rows.iter().map(|r| r["event"].as_str()).collect::<Vec<_>>(),
        ["rise", "transit", "set"]
    );
}
