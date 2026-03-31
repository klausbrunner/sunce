//! Solar position calculator CLI - entry point and output handling.

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

fn exit_with_cli_error(err: crate::error::CliError) -> ! {
    match err {
        crate::error::CliError::Exit(message) => {
            println!("{}", message);
            std::process::exit(0);
        }
        crate::error::CliError::Message(message) => {
            eprintln!("Error: {}", message);
            std::process::exit(1);
        }
        crate::error::CliError::MessageWithCode(message, code) => {
            eprintln!("Error: {}", message);
            std::process::exit(code);
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

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
                        Ok(true) => std::process::exit(0),
                        Ok(false) => std::process::exit(1),
                        Err(err) => {
                            eprintln!("Error: {}", err);
                            std::process::exit(2);
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
                                std::process::exit(1);
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
                    }
                    Err(err) => {
                        eprintln!("Error: {}", err);
                        std::process::exit(if predicate_mode { 2 } else { 1 });
                    }
                }
            }
            Err(err) => exit_with_cli_error(err),
        },
        Err(err) => exit_with_cli_error(err),
    }
}
