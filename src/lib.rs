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

pub fn run(args: Vec<String>) -> i32 {
    match cli::parse_cli(args) {
        Ok(parsed) => match validate::validate(parsed) {
            Ok(valid) => {
                let predicate_mode = matches!(valid, validate::ValidCommand::Predicate(_));
                match planner::build_plan(valid) {
                    Ok(planner::RunPlan::Predicate(job)) => match if job.wait {
                        predicate::wait_until_true(&job).map(|()| true)
                    } else {
                        predicate::run_once(&job)
                    } {
                        Ok(true) => 0,
                        Ok(false) => 1,
                        Err(err) => {
                            eprintln!("Error: {}", err);
                            2
                        }
                    },
                    Ok(planner::RunPlan::Stream(plan)) => {
                        let start = plan.params.perf.then(std::time::Instant::now);
                        let planner::ComputePlan {
                            data_iter,
                            command,
                            params,
                            allow_time_cache,
                            flush_each_record,
                        } = plan;

                        let results = compute::calculate_stream(
                            data_iter,
                            command,
                            params.clone(),
                            allow_time_cache,
                        );
                        let record_count = match output::dispatch_output(
                            results,
                            command,
                            &params,
                            flush_each_record,
                        ) {
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
                    Err(err) => {
                        eprintln!("Error: {}", err);
                        if predicate_mode { 2 } else { 1 }
                    }
                }
            }
            Err(err) => cli_error_exit_code(err),
        },
        Err(err) => cli_error_exit_code(err),
    }
}
