mod common;
use chrono::{Duration, FixedOffset, NaiveDate};
use common::{parse_csv_output_maps, parse_rfc3339, sunce_command};
use solar_positioning::{Horizon, HorizonState, Location, SolarEvents};

// Verify CLI integration across coordinates and seasons; numerical accuracy belongs to the library.
#[test]
fn worldwide_events_grid_matches_library() {
    let first_day = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    for lon in (-180..=180).step_by(30) {
        let longitude = lon.to_string();
        let timezone = format!("{:+03}:00", lon / 15);
        let output = sunce_command()
            .args([
                "-80:80:20",
                &longitude,
                "2024",
                "events",
                "--timezone",
                &timezone,
                "--deltat=69.184",
                "--format=csv",
                "--show-inputs",
                "--headers",
            ])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        let rows = parse_csv_output_maps(std::str::from_utf8(&output).unwrap());
        let mut rows = rows.iter();
        let zone = FixedOffset::east_opt(lon * 240).unwrap();
        for day_offset in 0..366 {
            let date = first_day + Duration::days(day_offset);
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
                expected.sort_by_key(|(_, time)| *time);
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
                                actual.timestamp(),
                                time.timestamp(),
                                "{lat}, {lon}, {date}"
                            );
                            assert_eq!(actual.offset(), &zone);
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
