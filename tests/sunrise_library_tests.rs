mod common;

use chrono::{Datelike, Duration, NaiveDate, Timelike};
use common::{parse_csv_output_maps, parse_rfc3339, sunce_command};
use solar_positioning::{Horizon, SunriseResult, spa};
use std::collections::HashSet;

// Test sunce's integration with the library, including SPA's polar limitations.
#[test]
fn worldwide_sunrise_grid_matches_library() {
    let first_day = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    for lon in (-180..=180).step_by(30) {
        let mut remaining: HashSet<_> = (-80..=80)
            .step_by(20)
            .flat_map(|lat| (0..366).map(move |day| (lat, first_day + Duration::days(day))))
            .collect();
        let longitude = lon.to_string();
        let timezone = format!("{:+03}:00", lon / 15);
        let output = sunce_command()
            .args([
                "-80:80:20",
                &longitude,
                "2024",
                "sunrise",
                "--timezone",
                &timezone,
                "--deltat=69.184",
                "--format=csv",
                "--show-inputs",
                "--headers",
            ])
            .output()
            .expect("sunce should run");
        assert!(
            output.status.success(),
            "sunrise grid at longitude {lon}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        for row in parse_csv_output_maps(std::str::from_utf8(&output.stdout).unwrap()) {
            let latitude: f64 = row["latitude"].parse().unwrap();
            let date = parse_rfc3339(&row["dateTime"]);
            assert_eq!(latitude, f64::from(latitude as i32));
            assert_eq!(row["longitude"].parse::<f64>().unwrap(), f64::from(lon));
            assert_eq!(date.offset().local_minus_utc(), lon * 240);
            assert_eq!(
                (date.year(), date.hour(), date.minute(), date.second()),
                (2024, 0, 0, 0)
            );
            assert!(
                remaining.remove(&(latitude as i32, date.date_naive())),
                "unexpected or duplicate input: {latitude}, {lon}, {date}"
            );

            let expected = spa::sunrise_sunset_for_horizon(
                date,
                latitude,
                f64::from(lon),
                69.184,
                Horizon::SunriseSunset,
            )
            .unwrap();
            let kind = match &expected {
                SunriseResult::RegularDay { .. } => "NORMAL",
                SunriseResult::AllDay { .. } => "ALL_DAY",
                SunriseResult::AllNight { .. } => "ALL_NIGHT",
            };
            assert_eq!(row["type"], kind, "{latitude}, {lon}, {date}");
            for (field, event) in [
                ("sunrise", expected.sunrise()),
                ("transit", Some(expected.transit())),
                ("sunset", expected.sunset()),
            ] {
                if let Some(event) = event {
                    // CLI timestamps omit fractional seconds.
                    assert_eq!(
                        parse_rfc3339(&row[field]).timestamp(),
                        event.timestamp(),
                        "{latitude}, {lon}, {date}: {field}"
                    );
                } else {
                    assert!(row[field].is_empty(), "unexpected {field}: {row:?}");
                }
            }
        }
        assert!(
            remaining.is_empty(),
            "missing {} days at longitude {lon}",
            remaining.len()
        );
    }
}
