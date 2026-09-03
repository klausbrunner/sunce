//! Solar position calculator application pipeline.

mod cli;
mod compute;
mod data;
mod error;
mod output;
#[cfg(feature = "parquet")]
mod parquet;
mod parsed;
mod position;
mod predicate;
mod sunrise;
mod validate;

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

fn run_stream(request: validate::StreamRequest) -> i32 {
    let validate::StreamRequest {
        command,
        source,
        params,
    } = request;
    let watch_mode = source.is_watch_mode(params.step.is_some());
    let allow_time_cache = !watch_mode;
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

    let results = compute::calculate_stream(data_iter, command, params.clone(), allow_time_cache);
    let record_count = match output::dispatch_output(results, command, &params, flush_each_record) {
        Ok(count) => count,
        Err(err) => {
            eprintln!("Error: {}", err);
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
    match cli::parse_cli(args).and_then(validate::validate) {
        Ok(valid) => execute(valid),
        Err(err) => cli_error_exit_code(err),
    }
}
