//! Solar position calculator application pipeline.

mod cli;
mod compute;
mod data;
mod error;
mod output;
#[cfg(feature = "parquet")]
mod parquet;
mod parsed;
mod planner;
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

fn run_stream(plan: planner::ComputePlan) -> i32 {
    let start = plan.params.perf.then(std::time::Instant::now);
    let planner::ComputePlan {
        data_iter,
        command,
        params,
        allow_time_cache,
        flush_each_record,
    } = plan;

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
    let error_code = if matches!(&valid, validate::ValidCommand::Predicate(_)) {
        2
    } else {
        1
    };

    match planner::build_plan(valid) {
        Ok(planner::RunPlan::Predicate(job)) => run_predicate(job),
        Ok(planner::RunPlan::Stream(plan)) => run_stream(plan),
        Err(err) => {
            eprintln!("Error: {}", err);
            error_code
        }
    }
}

pub fn run(args: Vec<String>) -> i32 {
    match cli::parse_cli(args).and_then(validate::validate) {
        Ok(valid) => execute(valid),
        Err(err) => cli_error_exit_code(err),
    }
}
