#![cfg(feature = "cli")]

mod common;
use common::{parse_csv_output_maps, parse_rfc3339, sunce_command};
use jiff::{
    ToSpan,
    civil::date,
    tz::{Offset, TimeZone},
};
use solar_positioning::{Horizon, HorizonState, Location, SolarEvents};

// Verify CLI integration across coordinates and seasons; numerical accuracy belongs to the library.
#[test]
fn worldwide_events_grid_matches_library() {
    let first_day = date(2024, 1, 1);
    // Sample throughout the year; focused tests cover unusual event days and DST.
    let dates: Vec<_> = (0..366)
        .step_by(30)
        .map(|offset| first_day + offset.days())
        .collect();
    let input = dates
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    for lon in (-180..=180).step_by(30) {
        let longitude = lon.to_string();
        let timezone = format!("{:+03}:00", lon / 15);
        let output = sunce_command()
            .args([
                "-80:80:20",
                &longitude,
                "@-",
                "events",
                "--timezone",
                &timezone,
                "--deltat=69.184",
                "--format=csv",
                "--show-inputs",
                "--headers",
            ])
            .write_stdin(input.clone())
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        let rows = parse_csv_output_maps(std::str::from_utf8(&output).unwrap());
        let mut rows = rows.iter();
        let offset = Offset::from_seconds(lon * 240).unwrap();
        let zone = offset.to_time_zone();
        for &date in &dates {
            for lat in (-80..=80).step_by(20) {
                let day = SolarEvents::new()
                    .for_date(
                        date,
                        &zone,
                        Location {
                            latitude: f64::from(lat),
                            longitude: f64::from(lon),
                        },
                        69.184,
                        Horizon::SunriseSunset,
                    )
                    .unwrap();
                let state = if !day.rises.is_empty() || !day.sets.is_empty() {
                    "CROSSING"
                } else {
                    match day.state_at_start {
                        HorizonState::Above => "ABOVE",
                        HorizonState::Below => "BELOW",
                        HorizonState::OnHorizon => "ON_HORIZON",
                    }
                };
                let mut expected = Vec::new();
                for (name, times) in [
                    ("sunrise", day.rises),
                    ("transit", day.transits),
                    ("sunset", day.sets),
                ] {
                    expected.extend(times.into_iter().map(|time| (Some(name), Some(time))));
                }
                expected.sort_by_key(|(_, time)| time.as_ref().map(|t| t.timestamp()));
                if expected.is_empty() {
                    expected.push((None, None));
                }
                for (event, time) in expected {
                    let row = rows.next().expect("missing event row");
                    assert_eq!(row["latitude"].parse::<f64>().unwrap(), f64::from(lat));
                    assert_eq!(row["longitude"].parse::<f64>().unwrap(), f64::from(lon));
                    assert_eq!(row["date"], date.to_string());
                    assert_eq!(row["day_state"], state);
                    assert_eq!(row["event"], event.unwrap_or(""));
                    match time {
                        Some(time) => {
                            let actual = parse_rfc3339(&row["time"]);
                            assert_eq!(
                                actual.timestamp().as_second(),
                                time.timestamp().as_second(),
                                "{lat}, {lon}, {date}"
                            );
                            assert_eq!(actual.offset(), offset);
                        }
                        None => assert!(row["time"].is_empty()),
                    }
                }
            }
        }
        assert!(
            rows.next().is_none(),
            "unexpected extra rows at longitude {lon}"
        );
    }
}

#[test]
fn selected_algorithm_and_horizon_match_library() {
    for (algorithm, calculator) in [
        ("spa", SolarEvents::new()),
        ("grena3", SolarEvents::grena3()),
    ] {
        for (horizon, rise, set) in [
            (Horizon::SunriseSunset, "sunrise", "sunset"),
            (Horizon::Custom(-4.5), "rise", "set"),
        ] {
            let mut command = sunce_command();
            command.args([
                "--timezone=UTC",
                "--deltat=69.184",
                "--format=csv",
                "52",
                "13.4",
                "2024-03-20",
                "events",
                "--algorithm",
                algorithm,
            ]);
            if let Horizon::Custom(angle) = horizon {
                command.arg(format!("--horizon={angle}"));
            }
            let output = command.assert().success().get_output().stdout.clone();
            let rows = parse_csv_output_maps(std::str::from_utf8(&output).unwrap());
            let actual: Vec<_> = rows
                .iter()
                .map(|row| {
                    (
                        row["event"].as_str(),
                        parse_rfc3339(&row["time"]).timestamp().as_second(),
                    )
                })
                .collect();

            let day = calculator
                .for_date(
                    date(2024, 3, 20),
                    &TimeZone::UTC,
                    Location {
                        latitude: 52.0,
                        longitude: 13.4,
                    },
                    69.184,
                    horizon,
                )
                .unwrap();
            let mut expected: Vec<_> = [
                (rise, day.rises),
                ("transit", day.transits),
                (set, day.sets),
            ]
            .into_iter()
            .flat_map(|(label, times)| {
                times
                    .into_iter()
                    .map(move |time| (label, time.timestamp().as_second()))
            })
            .collect();
            expected.sort_by_key(|(_, time)| *time);
            assert_eq!(actual, expected, "{algorithm}, {horizon:?}");
        }
    }
}
