//! Solar position calculator CLI - entry point and output handling.

mod cli;
mod compute;
mod data;
mod error;
mod output;
#[cfg(feature = "parquet")]
mod parquet;
mod planner;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    match cli::parse_cli(args) {
        Ok((source, command, params)) => {
            let start = params.perf.then(std::time::Instant::now);

            let planner::ComputePlan {
                data_iter,
                command,
                params,
                allow_time_cache,
                flush_each_record,
            } = match planner::build_job(source, command, params) {
                Ok(plan) => plan,
                Err(err) => {
                    eprintln!("Error: {}", err);
                    std::process::exit(1);
                }
            };

            let results =
                compute::calculate_stream(data_iter, command, params.clone(), allow_time_cache);
            let record_count =
                match output::dispatch_output(results, command, &params, flush_each_record) {
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
        Err(crate::error::CliError::Exit(message)) => {
            println!("{}", message);
            std::process::exit(0);
        }
        Err(crate::error::CliError::Message(message)) => {
            eprintln!("Error: {}", message);
            std::process::exit(1);
        }
    }
}
