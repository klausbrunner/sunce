//! Solar position calculator CLI - entry point and output handling.

mod compute;
mod data;
mod output;
#[cfg(feature = "parquet")]
mod parquet;

use data::DataSource;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    match data::parse_cli(args) {
        Ok((source, command, params)) => {
            // Performance monitoring setup
            let start = if params.perf {
                Some(std::time::Instant::now())
            } else {
                None
            };

            // Expand data source to iterator
            let data_iter = match &source {
                DataSource::Separate(loc_source, time_source) => data::expand_cartesian_product(
                    loc_source.clone(),
                    time_source.clone(),
                    params.step.clone(),
                    params.timezone.clone(),
                    command,
                ),
                DataSource::Paired(path) => {
                    data::expand_paired_file(path.clone(), params.timezone.clone())
                }
            }
            .unwrap_or_else(|err| {
                eprintln!("Error: {}", err);
                std::process::exit(1);
            });

            // Calculate results
            let results = compute::calculate_stream(data_iter, command, params.clone());

            // Write output in appropriate format
            let record_count = if params.format.to_uppercase() == "PARQUET" {
                #[cfg(feature = "parquet")]
                {
                    let stdout = std::io::stdout();
                    match output::write_parquet_output(results, command, &params, stdout) {
                        Ok(count) => count,
                        Err(e) => {
                            eprintln!("Error: {}", e);
                            std::process::exit(1);
                        }
                    }
                }
                #[cfg(not(feature = "parquet"))]
                {
                    eprintln!(
                        "Error: PARQUET format not available. Recompile with --features parquet"
                    );
                    std::process::exit(1);
                }
            } else {
                // Text-based formats (CSV, JSON, text)
                use std::io::{BufWriter, Write};
                let stdout = std::io::stdout();
                let mut writer = BufWriter::new(stdout.lock());
                let flush_each_record = source.uses_stdin() || source.is_watch_mode(&params.step);

                match output::write_formatted_output(
                    results,
                    command,
                    &params,
                    source.clone(),
                    &mut writer,
                    flush_each_record,
                ) {
                    Ok(count) => {
                        let _ = writer.flush();
                        count
                    }
                    Err(e) => {
                        let _ = writer.flush();
                        eprintln!("Error: {}", e);
                        std::process::exit(1);
                    }
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
            // Help and version output should not be prefixed with "Error:"
            if e.starts_with("sunce ") || e.starts_with("Usage: ") {
                println!("{}", e);
                std::process::exit(0);
            } else {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }
}
