//! Solar position calculator application pipeline.

use crate::{Error, Location, cli, data, input, output, predicate, validate};

fn cli_error_exit_code(err: crate::error::CliError) -> i32 {
    match err {
        crate::error::CliError::Exit(message) => {
            println!("{}", message);
            0
        }
        crate::error::CliError::Message(message) => {
            eprintln!("Error: {}", message);
            1
        }
        crate::error::CliError::MessageWithCode(message, code) => {
            eprintln!("Error: {}", message);
            code
        }
    }
}

fn run_predicate(job: predicate::PredicateJob) -> i32 {
    let result = if job.wait {
        predicate::wait_until_true(&job).map(|()| true)
    } else {
        predicate::run_once(&job)
    };

    match result {
        Ok(true) => 0,
        Ok(false) => 1,
        Err(err) => {
            eprintln!("Error: {}", err);
            2
        }
    }
}

fn export_options(
    params: &data::Parameters,
    format: output::Format,
    flush_each_record: bool,
) -> output::Options {
    output::Options {
        format,
        headers: params.output.headers,
        show_inputs: params.output.should_show_inputs(),
        elevation_angle: params.output.elevation_angle,
        flush_each_record,
    }
}

fn run_stream(request: validate::StreamRequest) -> i32 {
    let validate::StreamRequest {
        command,
        source,
        params,
    } = request;
    if params.verbose {
        let operation = match command {
            data::Command::Position => "positions",
            data::Command::Events => "events",
        };
        eprintln!(
            "Calculating {operation} using {}; output: {}.",
            params.calculation.algorithm, params.output.format
        );
    }
    let watch_mode = source.is_watch_mode(params.step.is_some());
    let flush_each_record = source.uses_stdin() || watch_mode;
    let data_iter = match source {
        data::DataSource::Separate(locations, times) => data::expand_cartesian_product(
            locations,
            times,
            params.step,
            params.timezone.clone(),
            command,
        ),
        data::DataSource::Paired(path) => data::expand_paired_file(path, params.timezone.clone()),
    };
    let data_iter = match data_iter {
        Ok(iter) => iter,
        Err(err) => {
            eprintln!("Error: {}", err);
            return 1;
        }
    };
    let start = params.perf.then(std::time::Instant::now);

    let export = || -> Result<usize, Error> {
        match command {
            data::Command::Position => {
                let input = data_iter.map(|item| {
                    item.map(|(latitude, longitude, time)| input::PositionInput {
                        location: Location {
                            latitude,
                            longitude,
                        },
                        datetime: time.datetime,
                    })
                    .map_err(Error::Input)
                });
                let rows = crate::positions(input, params.position_options())?;
                match params.output.format {
                    data::OutputFormat::Standard(format) => output::write_positions(
                        rows,
                        &params.environment,
                        export_options(&params, format, flush_each_record),
                        std::io::stdout(),
                    ),
                    #[cfg(feature = "parquet")]
                    data::OutputFormat::Parquet => crate::parquet::write_positions(
                        rows,
                        &params.environment,
                        crate::parquet::Options {
                            show_inputs: params.output.should_show_inputs(),
                            elevation_angle: params.output.elevation_angle,
                        },
                        std::io::stdout(),
                    ),
                }
            }
            data::Command::Events => {
                let input = data_iter.map(|item| {
                    item.map(|(latitude, longitude, time)| input::EventInput {
                        location: Location {
                            latitude,
                            longitude,
                        },
                        date: time.datetime.date_naive(),
                        timezone: time.zone,
                    })
                    .map_err(Error::Input)
                });
                let rows = crate::events(input, params.calculation)?;
                match params.output.format {
                    data::OutputFormat::Standard(format) => output::write_events(
                        rows,
                        export_options(&params, format, flush_each_record),
                        std::io::stdout(),
                    ),
                    #[cfg(feature = "parquet")]
                    data::OutputFormat::Parquet => crate::parquet::write_events(
                        rows,
                        params.output.should_show_inputs(),
                        std::io::stdout(),
                    ),
                }
            }
        }
    };
    let record_count = match export() {
        Ok(count) => count,
        Err(err) => {
            eprintln!("Error: {err}");
            return 1;
        }
    };

    if let Some(start_time) = start {
        let elapsed = start_time.elapsed();
        eprintln!(
            "Processed {} records in {:.3}s ({:.0} records/sec)",
            record_count,
            elapsed.as_secs_f64(),
            record_count as f64 / elapsed.as_secs_f64()
        );
    } else if params.verbose {
        eprintln!("Processed {record_count} records.");
    }
    0
}

fn execute(valid: validate::ValidCommand) -> i32 {
    match valid {
        validate::ValidCommand::Predicate(job) => run_predicate(job),
        validate::ValidCommand::Stream(request) => run_stream(request),
    }
}

pub fn run(args: Vec<String>) -> i32 {
    // Detect predicate intent before parsing so even errors preceding the flag
    // cannot be mistaken for a false predicate by a calling script.
    let args = cli::Arguments::new(&args);
    let predicate_requested = args.predicate_requested();
    match cli::parse_cli(args).and_then(validate::validate) {
        Ok(valid) => execute(valid),
        Err(crate::error::CliError::Message(message)) if predicate_requested => {
            cli_error_exit_code(crate::error::predicate_error(message))
        }
        Err(err) => cli_error_exit_code(err),
    }
}
