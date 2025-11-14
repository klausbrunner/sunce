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
            // Performance monitoring setup
            let start = if params.perf {
                Some(std::time::Instant::now())
            } else {
                None
            };

            let (compute_plan, output_plan) = match planner::build_job(source, command, params) {
                Ok(spec) => spec,
                Err(err) => {
                    eprintln!("Error: {}", err);
                    std::process::exit(1);
                }
            };

            let planner::ComputePlan {
                data_iter,
                command,
                params,
                allow_time_cache,
            } = compute_plan;

            let results =
                compute::calculate_stream(data_iter, command, params.clone(), allow_time_cache);

            let record_count =
                match output::dispatch_output(results, command, &params, &output_plan) {
                    Ok(count) => count,
                    Err(err) => {
                        eprintln!("Error: {}", err);
                        std::process::exit(1);
                    }
                };

            // Report performance if requested
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
        Err(e) => {
            let message = e.to_string();
            if message.starts_with("sunce ") || message.starts_with("Usage: ") {
                println!("{}", message);
                std::process::exit(0);
            } else {
                eprintln!("Error: {}", message);
                std::process::exit(1);
            }
        }
    }
}
