use clap::{Arg, ArgAction, Command};

pub fn build_cli() -> Command {
    Command::new("sunce")
        .version(env!("CARGO_PKG_VERSION"))
        .about("Computes solar coordinates and events (sunrise, sunset, transit, twilight)")
        .subcommand_required(true)
        .long_about(Some(concat!(
            "High-performance command-line solar position calculator\n\n",
            "Computes topocentric solar coordinates and solar events (sunrise, sunset, transit, twilight).\n",
            "Supports time series, geographic sweeps, file input, and streaming with CSV/JSON output.\n\n",
            "Examples:\n",
            "  sunce 52.0 13.4 2024-01-01 position\n",
            "  sunce 52:53:0.1 13:14:0.1 2024 position --format=csv\n",
            "  sunce @coords.txt @times.txt position\n",
            "  sunce @data.txt position  # paired lat,lng,datetime data\n",
            "  echo -e '52.0 13.4\\n52.1 13.5' | sunce @- now position"
        )))
        .arg(Arg::new("latitude")
            .help(concat!(
                "Latitude: decimal degrees, range, or file\n",
                "  52.5        single coordinate\n",
                "  52:53:0.1   range from 52° to 53° in 0.1° steps\n",
                "  @coords.txt file with coordinates (or @- for stdin)"
            ))
            .required(true)
            .allow_hyphen_values(true)
            .index(1))
        .arg(Arg::new("longitude")
            .help(concat!(
                "Longitude: decimal degrees, range, or file\n",
                "  13.4        single coordinate\n",
                "  13:14:0.1   range from 13° to 14° in 0.1° steps\n",
                "  @coords.txt file with coordinates (or @- for stdin)"
            ))
            .required(false)
            .allow_hyphen_values(true)
            .index(2))
        .arg(Arg::new("dateTime")
            .help(concat!(
                "Date/time: ISO format, partial dates, unix timestamps, or file\n",
                "  2024-01-01           specific date (midnight)\n",
                "  2024-01-01T12:00:00  specific date and time\n",
                "  2024                 entire year (with --step)\n",
                "  now                  current date and time\n",
                "  1577836800           unix timestamp (seconds since 1970, UTC)\n",
                "  @times.txt           file with times (or @- for stdin)\n",
                "                       (files require explicit dates like 2024-01-15)"
            ))
            .required(false)
            .index(3))

        // Global options
        .arg(Arg::new("deltat")
            .long("deltat")
            .help("Delta T in seconds; an estimate is used if this option is given without a value.")
            .num_args(0..=1)
            .require_equals(true)
            .value_name("deltaT"))
        .arg(Arg::new("format")
            .long("format")
            .help("Output format, one of HUMAN, CSV, JSON.")
            .require_equals(true)
            .value_name("format"))
        .arg(Arg::new("headers")
            .long("headers")
            .action(ArgAction::SetTrue)
            .help("Show headers in output (CSV only). Default: true"))
        .arg(Arg::new("no-headers")
            .long("no-headers")
            .action(ArgAction::SetTrue)
            .help("Don't show headers in output (CSV only)"))
        .arg(Arg::new("show-inputs")
            .long("show-inputs")
            .action(ArgAction::SetTrue)
            .help("Show all inputs in output. Automatically enabled for coordinate ranges, time series, files unless --no-show-inputs is used."))
        .arg(Arg::new("no-show-inputs")
            .long("no-show-inputs")
            .action(ArgAction::SetTrue)
            .help("Don't show inputs in output"))
        .arg(Arg::new("timezone")
            .long("timezone")
            .help("Timezone as offset (e.g. +01:00) and/or zone id (e.g. America/Los_Angeles). Overrides any timezone info found in dateTime.")
            .require_equals(true)
            .value_name("timezone"))
        .arg(Arg::new("perf")
            .long("perf")
            .action(ArgAction::SetTrue)
            .hide(true)
            .help("Show performance statistics."))

        // Commands
        .subcommand(
            Command::new("position")
                .about("Calculates topocentric solar coordinates.")
                .arg(Arg::new("elevation-angle")
                    .long("elevation-angle")
                    .action(ArgAction::SetTrue)
                    .help("Output elevation angle instead of zenith angle."))
                .arg(Arg::new("refraction")
                    .long("refraction")
                    .action(ArgAction::SetTrue)
                    .help("Apply refraction correction. Default: true"))
                .arg(Arg::new("no-refraction")
                    .long("no-refraction")
                    .action(ArgAction::SetTrue)
                    .help("Don't apply refraction correction"))
                .arg(Arg::new("algorithm")
                    .short('a')
                    .long("algorithm")
                    .help("One of SPA, GRENA3. Default: spa.")
                    .require_equals(true)
                    .value_name("algorithm"))
                .arg(Arg::new("elevation")
                    .long("elevation")
                    .help("Elevation above sea level, in meters. Default: 0.")
                    .require_equals(true)
                    .value_name("elevation"))
                .arg(Arg::new("pressure")
                    .long("pressure")
                    .help("Avg. air pressure in millibars/hectopascals. Used for refraction correction. Default: 1013.")
                    .require_equals(true)
                    .value_name("pressure"))
                .arg(Arg::new("step")
                    .long("step")
                    .help("Step interval for time series. Examples: 30s, 15m, 2h. Default: 1h.")
                    .require_equals(true)
                    .value_name("step"))
                .arg(Arg::new("temperature")
                    .long("temperature")
                    .help("Avg. air temperature in degrees Celsius. Used for refraction correction. Default: 15.")
                    .require_equals(true)
                    .value_name("temperature"))
        )
        .subcommand(
            Command::new("sunrise")
                .about("Calculates sunrise, transit, sunset and (optionally) twilight times.")
                .arg(Arg::new("twilight")
                    .long("twilight")
                    .action(ArgAction::SetTrue)
                    .help("Show twilight times."))
        )
}
