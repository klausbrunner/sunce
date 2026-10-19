use chrono::DateTime;
use std::cell::Cell;
use std::io::{self, BufRead, Cursor, Read, Write};
use sunce::input::{self, CoordinateGrid, EventInput, PositionInput};
use sunce::output::{self, Format, Options};
use sunce::{
    CalculationAlgorithm, Error, EventOptions, Location, PositionOptions, PositionRecord, events,
    positions,
};

fn location() -> Location {
    Location {
        latitude: 52.52,
        longitude: 13.405,
    }
}

fn position() -> PositionInput {
    PositionInput {
        location: location(),
        datetime: DateTime::parse_from_rfc3339("2026-06-21T12:00:00+02:00").unwrap(),
    }
}

fn position_request() -> impl Iterator<Item = Result<PositionRecord, Error>> {
    positions([Ok(position())], PositionOptions::default()).unwrap()
}

#[test]
fn borrowed_inputs_are_lazy_and_results_keep_full_precision() {
    let values = [position(), position()];
    let seen = Cell::new(0);
    let input = values
        .iter()
        .copied()
        .inspect(|_| seen.set(seen.get() + 1))
        .map(Ok);
    let mut settings = PositionOptions {
        deltat: Some(69.184),
        ..PositionOptions::default()
    };
    settings.environment.refraction = false;
    let request = positions(input, settings).unwrap();
    assert_eq!(seen.get(), 0);
    let mut results = request;
    assert_eq!(seen.get(), 0);
    let actual = results.next().unwrap().unwrap().position;
    let expected = solar_positioning::SolarPositions::new()
        .at(&position().datetime, location(), 0.0, 69.184, None)
        .unwrap();
    assert_eq!(actual.azimuth(), expected.azimuth());
    assert_eq!(actual.zenith_angle(), expected.zenith_angle());
    assert_ne!(
        actual.azimuth(),
        (actual.azimuth() * 10_000.0).round() / 10_000.0
    );
    drop(results);
    assert_eq!(
        seen.get(),
        1,
        "dropping results must leave remaining input untouched"
    );
}

#[test]
fn settings_and_records_are_validated_at_their_boundaries() {
    let invalid = PositionOptions {
        deltat: Some(f64::NAN),
        ..PositionOptions::default()
    };
    assert!(matches!(
        positions(std::iter::empty(), invalid),
        Err(Error::Input(_))
    ));
    let mut invalid = PositionOptions {
        algorithm: CalculationAlgorithm::Grena3,
        ..Default::default()
    };
    invalid.environment.elevation = 100.0;
    assert!(matches!(
        positions(std::iter::empty(), invalid),
        Err(Error::Input(_))
    ));

    let mut invalid = position();
    invalid.location.latitude = 91.0;
    let mut request = positions([Ok(invalid)], PositionOptions::default()).unwrap();
    assert!(matches!(request.next().unwrap(), Err(Error::Input(_))));
}

#[test]
fn grids_validate_steps_and_preserve_arithmetic_coordinates() {
    for latitude in [
        (50.0, 51.0, 0.0),
        (50.0, 51.0, -0.1),
        (50.0, 51.0, f64::NAN),
        (50.0, 51.0, 1e-30),
    ] {
        assert!(CoordinateGrid::new(latitude, (13.0, 13.0, 0.0)).is_err());
    }
    let points = CoordinateGrid::new((53.0, 52.0, -0.5), (13.0, 13.0, 0.0))
        .unwrap()
        .locations()
        .collect::<Vec<_>>();
    assert_eq!(
        points.iter().map(|p| p.latitude).collect::<Vec<_>>(),
        [53.0, 52.5, 52.0]
    );
    let points = CoordinateGrid::new((0.0, 1.0, 0.6), (0.0, 0.0, 0.0))
        .unwrap()
        .locations()
        .collect::<Vec<_>>();
    assert_eq!(points.len(), 2, "a range must not invent an extra endpoint");
}

#[test]
fn readers_accept_borrowed_buffers_and_report_physical_line_numbers() {
    let text = String::from("# locations\n\n52.52,13.405,2026-06-21T12:00:00+02:00\nbroken\n");
    let input = input::read_positions(Cursor::new(text.as_bytes()), "UTC".parse().unwrap());
    let mut results = positions(input, PositionOptions::default()).unwrap();
    let datetime = results.next().unwrap().unwrap().datetime;
    assert_eq!(datetime.to_rfc3339(), "2026-06-21T10:00:00+00:00");
    let err = results.next().unwrap().unwrap_err();
    assert!(matches!(err, Error::Input(_)));
    assert!(err.to_string().contains("input:4: expected 3 fields"));

    let coordinates = String::from("# coordinates\n52.52,13.405\n");
    assert_eq!(
        input::read_locations(Cursor::new(coordinates.as_bytes()))
            .next()
            .unwrap()
            .unwrap()
            .latitude,
        52.52
    );
    assert!(
        input::read_times(Cursor::new(b"now\n"), "UTC".parse().unwrap())
            .next()
            .unwrap()
            .is_err()
    );
}

#[test]
fn elapsed_time_sampling_and_calendar_dates_preserve_dst() {
    let timezone = "Europe/Berlin".parse().unwrap();
    for (date, expected) in [("2026-03-29", 138), ("2026-10-25", 150)] {
        let times = input::position_times(date, timezone, "10m".parse().unwrap())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(times.len(), expected);
        assert_ne!(
            times.first().unwrap().offset(),
            times.last().unwrap().offset()
        );
    }
    assert!(
        input::position_times(
            "2026-06-21",
            timezone,
            sunce::Step(chrono::Duration::zero())
        )
        .is_err()
    );

    let dates =
        input::event_dates("2026-03-28".parse().unwrap(), "2026-03-30".parse().unwrap()).unwrap();
    let records = input::grid_events(
        CoordinateGrid::new((52.52, 52.52, 0.0), (13.405, 13.405, 0.0)).unwrap(),
        dates,
        timezone,
    );
    let events = events(records.map(Ok), EventOptions::default())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let sunrise: Vec<_> = events
        .into_iter()
        .filter(|row| row.event == Some("sunrise"))
        .map(|row| row.time.unwrap())
        .collect();
    assert_eq!(sunrise.len(), 3);
    assert_eq!(sunrise[0].offset().local_minus_utc(), 3600);
    assert_eq!(sunrise[1].offset().local_minus_utc(), 7200);
    assert_eq!(sunrise[2].date_naive().to_string(), "2026-03-30");
}

#[test]
fn event_dates_do_not_require_an_existing_midnight() {
    let source = "-23.55,-46.63,2018-11-04\n";
    let input = input::read_events(
        Cursor::new(source.as_bytes()),
        "America/Sao_Paulo".parse().unwrap(),
    );
    let rows = events(input, EventOptions::default())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(rows.iter().any(|row| row.event == Some("sunrise")));

    let input = EventInput {
        location: Location {
            latitude: -23.55,
            longitude: -46.63,
        },
        date: "2018-11-04".parse().unwrap(),
        timezone: "America/Sao_Paulo".parse().unwrap(),
    };
    let rows = events([Ok(input)], EventOptions::default())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(rows.iter().any(|row| row.event == Some("sunrise")));

    let input = EventInput {
        location: Location {
            latitude: -13.83,
            longitude: -171.75,
        },
        date: "2011-12-30".parse().unwrap(),
        timezone: "Pacific/Apia".parse().unwrap(),
    };
    let rows = events([Ok(input)], EventOptions::default())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        matches!(rows.as_slice(), [row] if row.day_state == sunce::DayState::Empty && row.event.is_none())
    );
}

#[test]
fn position_periods_cover_midnight_transitions() {
    for (zone, date, count, first, last) in [
        (
            "America/Santiago",
            "2026-04-04",
            25,
            "2026-04-04T00:00:00-03:00",
            "2026-04-04T23:00:00-04:00",
        ),
        (
            "America/Sao_Paulo",
            "2018-11-04",
            23,
            "2018-11-04T01:00:00-02:00",
            "2018-11-04T23:00:00-02:00",
        ),
        (
            "America/Sao_Paulo",
            "2018-11-03",
            24,
            "2018-11-03T00:00:00-03:00",
            "2018-11-03T23:00:00-03:00",
        ),
        (
            "America/Havana",
            "2026-11-01",
            25,
            "2026-11-01T00:00:00-04:00",
            "2026-11-01T23:00:00-05:00",
        ),
    ] {
        let times = input::position_times(date, zone.parse().unwrap(), "1h".parse().unwrap())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(times.len(), count, "{zone} {date}");
        assert_eq!(times[0].to_rfc3339(), first);
        assert_eq!(times.last().unwrap().to_rfc3339(), last);
        assert!(
            times
                .windows(2)
                .all(|pair| (pair[1] - pair[0]).num_seconds() == 3600)
        );
        assert!(
            times
                .iter()
                .all(|time| time.date_naive().to_string() == date)
        );
    }
}

#[test]
fn position_periods_skip_nonexistent_calendar_dates() {
    let zone = "Pacific/Apia".parse().unwrap();
    let times = input::position_times("2011-12-30", zone, "1h".parse().unwrap())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(times.is_empty());
    let times = input::position_times("2011-12", zone, "1h".parse().unwrap())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(times.len(), 30 * 24);
    assert!(
        !times
            .iter()
            .any(|time| time.date_naive().to_string() == "2011-12-30")
    );
}

#[test]
fn date_only_inputs_advance_through_midnight_gaps_but_explicit_times_stay_strict() {
    let zone = "America/Sao_Paulo".parse().unwrap();
    let mut times = input::read_times(Cursor::new(b"2018-11-04\n2018-11-04T00:30:00\n"), zone);
    assert_eq!(
        times.next().unwrap().unwrap().to_rfc3339(),
        "2018-11-04T01:00:00-02:00"
    );
    assert!(times.next().unwrap().is_err());
}

struct BrokenReader;
impl Read for BrokenReader {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::other("reader failed"))
    }
}
impl BufRead for BrokenReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        Err(io::Error::other("reader failed"))
    }
    fn consume(&mut self, _: usize) {}
}

struct BrokenWriter {
    fail_flush: bool,
}
impl Write for BrokenWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.fail_flush {
            Ok(bytes.len())
        } else {
            Err(io::Error::other("write failed"))
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        Err(io::Error::other("flush failed"))
    }
}

#[test]
fn reader_writer_and_final_flush_failures_are_returned() {
    let records = input::read_positions(BrokenReader, "UTC".parse().unwrap());
    let error = positions(records, PositionOptions::default())
        .unwrap()
        .next()
        .unwrap()
        .unwrap_err();
    assert!(matches!(error, Error::Input(_)));
    assert!(error.to_string().contains("reader failed"));
    for fail_flush in [false, true] {
        let error = output::write_positions(
            position_request(),
            &sunce::Environment::default(),
            Options {
                format: Format::Csv,
                ..Default::default()
            },
            BrokenWriter { fail_flush },
        )
        .unwrap_err();
        assert!(matches!(error, Error::Output(_)));
        assert_eq!(
            error.to_string(),
            if fail_flush {
                "flush failed"
            } else {
                "write failed"
            }
        );
    }
}

#[test]
fn exporters_preserve_input_errors_and_use_caller_owned_memory() {
    let formats = [Format::Text, Format::Csv, Format::Json];
    for format in formats {
        let mut bytes = Vec::new();
        let count = output::write_positions(
            position_request(),
            &sunce::Environment::default(),
            Options {
                format,
                ..Default::default()
            },
            &mut bytes,
        )
        .unwrap();
        assert_eq!(count, 1);
        assert!(!bytes.is_empty());
        let request = positions(
            [Err(Error::Calculation("source failed".into()))],
            PositionOptions::default(),
        )
        .unwrap();
        let error = output::write_positions(
            request,
            &sunce::Environment::default(),
            Options {
                format,
                ..Default::default()
            },
            Vec::new(),
        )
        .unwrap_err();
        assert_eq!(error, Error::Calculation("source failed".into()));
    }
}

#[cfg(feature = "cli")]
#[test]
fn position_grid_exports_match_the_cli() {
    let formats = formats();
    for algorithm in [CalculationAlgorithm::Spa, CalculationAlgorithm::Grena3] {
        for &format in &formats {
            let grid = CoordinateGrid::new((52.0, 52.5, 0.5), (13.0, 14.0, 0.5)).unwrap();
            let times = input::position_times(
                "2026-03-29",
                "Europe/Berlin".parse().unwrap(),
                "6h".parse().unwrap(),
            )
            .unwrap();
            let mut settings = PositionOptions {
                deltat: Some(69.184),
                ..PositionOptions::default()
            };
            settings.algorithm = algorithm;
            let request = positions(input::grid_positions(grid, times), settings).unwrap();
            let options = Options {
                format: format.parse().unwrap_or_default(),
                show_inputs: true,
                elevation_angle: true,
                ..Default::default()
            };
            let mut bytes = Vec::new();
            export_positions(request, settings, format, options, &mut bytes).unwrap();
            let cli = std::process::Command::new(assert_cmd::cargo::cargo_bin!("sunce"))
                .args([
                    "--format",
                    format,
                    "--algorithm",
                    &algorithm.to_string(),
                    "--deltat=69.184",
                    "--timezone=Europe/Berlin",
                    "--elevation-angle",
                    "52:52.5:0.5",
                    "13:14:0.5",
                    "2026-03-29",
                    "position",
                    "--step=6h",
                ])
                .output()
                .unwrap();
            assert!(
                cli.status.success(),
                "{}",
                String::from_utf8_lossy(&cli.stderr)
            );
            assert_eq!(bytes, cli.stdout, "position {algorithm} {format}");
        }
    }
}

#[cfg(feature = "cli")]
#[test]
fn event_grid_exports_match_the_cli_across_dst() {
    let formats = formats();
    for &format in &formats {
        let grid = CoordinateGrid::new((52.0, 53.0, 1.0), (13.0, 13.0, 0.0)).unwrap();
        let dates = input::event_dates(
            chrono::NaiveDate::from_ymd_opt(2026, 3, 1).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2026, 3, 31).unwrap(),
        )
        .unwrap();
        let records = input::grid_events(grid, dates, "Europe/Berlin".parse().unwrap());
        let mut settings = EventOptions {
            deltat: Some(69.184),
            ..EventOptions::default()
        };
        settings.twilight = true;
        let request = events(records.map(Ok), settings).unwrap();
        let mut bytes = Vec::new();
        export_events(
            request,
            format,
            Options {
                format: format.parse().unwrap_or_default(),
                show_inputs: true,
                ..Default::default()
            },
            &mut bytes,
        )
        .unwrap();
        let cli = std::process::Command::new(assert_cmd::cargo::cargo_bin!("sunce"))
            .args([
                "--format",
                format,
                "--deltat=69.184",
                "--timezone=Europe/Berlin",
                "52:53:1",
                "13",
                "2026-03",
                "events",
                "--twilight",
            ])
            .output()
            .unwrap();
        assert!(
            cli.status.success(),
            "{}",
            String::from_utf8_lossy(&cli.stderr)
        );
        assert_eq!(bytes, cli.stdout, "events {format}");
    }
}

#[cfg(feature = "cli")]
fn formats() -> Vec<&'static str> {
    vec![
        "text",
        "csv",
        "json",
        #[cfg(feature = "parquet")]
        "parquet",
    ]
}

#[cfg(feature = "cli")]
#[test]
fn cli_uses_library_event_options_for_delta_t_horizons_and_twilight() {
    for algorithm in [CalculationAlgorithm::Spa, CalculationAlgorithm::Grena3] {
        for (deltat, horizon, twilight) in [(Some(69.184), Some(-6.0), false), (None, None, true)] {
            let settings = EventOptions {
                algorithm,
                deltat,
                horizon,
                twilight,
            };
            let records = events(
                [Ok(EventInput {
                    location: location(),
                    date: "2026-03-29".parse().unwrap(),
                    timezone: "Europe/Berlin".parse().unwrap(),
                })],
                settings,
            )
            .unwrap();
            let mut expected = Vec::new();
            output::write_events(
                records,
                Options {
                    format: Format::Json,
                    show_inputs: true,
                    ..Default::default()
                },
                &mut expected,
            )
            .unwrap();
            let mut cli = std::process::Command::new(assert_cmd::cargo::cargo_bin!("sunce"));
            cli.args([
                "52.52",
                "13.405",
                "2026-03-29",
                "events",
                "--format=json",
                "--show-inputs",
                "--timezone=Europe/Berlin",
            ])
            .arg(format!("--algorithm={algorithm}"))
            .arg(deltat.map_or_else(|| "--deltat".into(), |value| format!("--deltat={value}")));
            if let Some(horizon) = horizon {
                cli.arg(format!("--horizon={horizon}"));
            }
            if twilight {
                cli.arg("--twilight");
            }
            let actual = cli.output().unwrap();
            assert!(
                actual.status.success(),
                "{}",
                String::from_utf8_lossy(&actual.stderr)
            );
            assert_eq!(expected, actual.stdout, "{settings:?}");
        }
    }
}

#[cfg(feature = "cli")]
fn export_positions(
    rows: impl Iterator<Item = Result<PositionRecord, Error>>,
    settings: PositionOptions,
    format: &str,
    options: Options,
    bytes: &mut Vec<u8>,
) -> Result<usize, Error> {
    #[cfg(feature = "parquet")]
    if format == "parquet" {
        return sunce::parquet::write_positions(
            rows,
            &settings.environment,
            sunce::parquet::Options {
                show_inputs: options.show_inputs,
                elevation_angle: options.elevation_angle,
            },
            bytes,
        );
    }
    let _ = format;
    output::write_positions(rows, &settings.environment, options, bytes)
}

#[cfg(feature = "cli")]
fn export_events(
    rows: impl Iterator<Item = Result<sunce::EventRecord, Error>>,
    format: &str,
    options: Options,
    bytes: &mut Vec<u8>,
) -> Result<usize, Error> {
    #[cfg(feature = "parquet")]
    if format == "parquet" {
        return sunce::parquet::write_events(rows, options.show_inputs, bytes);
    }
    let _ = format;
    output::write_events(rows, options, bytes)
}

#[test]
fn grids_check_counts_and_yield_large_ranges_without_materializing_them() {
    assert!(CoordinateGrid::new((0.0, 1.0, 1e-30), (0.0, 0.0, 0.0)).is_err());
    let grid = CoordinateGrid::new((-90.0, 90.0, 0.00001), (-180.0, 180.0, 0.00001)).unwrap();
    let first = grid.locations().next().unwrap();
    assert_eq!(first.latitude, -90.0);
    assert_eq!(first.longitude, -180.0);
    let input = input::grid_positions(grid, [Err(Error::Input("bad time".into()))]);
    let errors: Vec<_> = input.collect();
    assert_eq!(errors.len(), 1);
    assert_eq!(
        errors[0].as_ref().unwrap_err(),
        &Error::Input("bad time".into())
    );
}

struct LocalWriter(std::rc::Rc<std::cell::RefCell<Vec<u8>>>);
impl Write for LocalWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.borrow_mut().write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn composed_and_collected_results_export_to_single_threaded_writers() {
    let options = PositionOptions::default();
    for format in [Format::Text, Format::Csv, Format::Json] {
        let rows = positions([position(); 3].map(Ok), options)
            .unwrap()
            .filter(|row| row.as_ref().is_ok_and(|row| row.location.latitude > 50.0))
            .take(2)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let bytes = std::rc::Rc::default();
        let count = output::write_positions(
            rows.into_iter().map(Ok),
            &options.environment,
            Options {
                format,
                ..Default::default()
            },
            LocalWriter(std::rc::Rc::clone(&bytes)),
        )
        .unwrap();
        assert_eq!(count, 2);
        assert!(!bytes.borrow().is_empty());

        let input = EventInput {
            location: location(),
            date: "2026-06-21".parse().unwrap(),
            timezone: "UTC".parse().unwrap(),
        };
        let rows = events([Ok(input)], EventOptions::default())
            .unwrap()
            .filter(|row| row.as_ref().is_ok_and(|row| row.event == Some("sunrise")))
            .take(1);
        let count = output::write_events(
            rows,
            Options {
                format,
                ..Default::default()
            },
            LocalWriter(bytes),
        )
        .unwrap();
        assert_eq!(count, 1);
    }
}

#[test]
fn both_operations_preserve_upstream_error_variants() {
    for error in [
        Error::Input("input".into()),
        Error::Calculation("calculation".into()),
        Error::Output("output".into()),
    ] {
        assert_eq!(
            positions([Err(error.clone())], PositionOptions::default())
                .unwrap()
                .next()
                .unwrap()
                .unwrap_err(),
            error
        );
        assert_eq!(
            events([Err(error.clone())], EventOptions::default())
                .unwrap()
                .next()
                .unwrap()
                .unwrap_err(),
            error
        );
        for format in [Format::Text, Format::Csv, Format::Json] {
            let rows = events([Err(error.clone())], EventOptions::default()).unwrap();
            assert_eq!(
                output::write_events(
                    rows,
                    Options {
                        format,
                        ..Default::default()
                    },
                    Vec::new()
                )
                .unwrap_err(),
                error
            );
        }
        #[cfg(feature = "parquet")]
        {
            let rows = positions([Err(error.clone())], PositionOptions::default()).unwrap();
            assert_eq!(
                sunce::parquet::write_positions(
                    rows,
                    &sunce::Environment::default(),
                    sunce::parquet::Options::default(),
                    Vec::new()
                )
                .unwrap_err(),
                error
            );
            let rows = events([Err(error.clone())], EventOptions::default()).unwrap();
            assert_eq!(
                sunce::parquet::write_events(rows, false, Vec::new()).unwrap_err(),
                error
            );
        }
    }
}

#[test]
fn event_options_reject_invalid_combinations_before_reading_input() {
    for options in [
        EventOptions {
            deltat: Some(f64::NAN),
            ..Default::default()
        },
        EventOptions {
            horizon: Some(91.0),
            ..Default::default()
        },
        EventOptions {
            horizon: Some(f64::NAN),
            ..Default::default()
        },
        EventOptions {
            horizon: Some(-6.0),
            twilight: true,
            ..Default::default()
        },
    ] {
        assert!(events(std::iter::empty(), options).is_err());
    }
}
