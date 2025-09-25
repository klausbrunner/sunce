#[cfg(feature = "parquet")]
use crate::parquet_output::output_sunrise_results_parquet;
use crate::types::{OutputFormat, format_datetime_solarpos};
use chrono::{DateTime, FixedOffset};
use solar_positioning::types::SunriseResult;
use std::io::{self, BufWriter, Write};

const DATE_FORMAT: &str = "%Y-%m-%d";
const TIME_FORMAT: &str = "%H:%M:%S%:z";

pub struct SunriseResultData {
    pub datetime: DateTime<FixedOffset>,
    pub latitude: f64,
    pub longitude: f64,
    pub delta_t: f64,
    pub sunrise_result: SunriseResult<DateTime<FixedOffset>>,
    pub twilight_results: Option<TwilightResults>,
}

pub struct TwilightResults {
    pub civil: SunriseResult<DateTime<FixedOffset>>,
    pub nautical: SunriseResult<DateTime<FixedOffset>>,
    pub astronomical: SunriseResult<DateTime<FixedOffset>>,
}

pub fn output_sunrise_results<I>(
    results: I,
    format: &OutputFormat,
    show_inputs: bool,
    show_headers: bool,
    show_twilight: bool,
    is_stdin: bool,
) where
    I: Iterator<Item = SunriseResultData>,
{
    let stdout = io::stdout().lock();
    let mut writer = BufWriter::new(stdout);

    let result = match format {
        OutputFormat::Parquet => {
            #[cfg(feature = "parquet")]
            {
                let stdout = io::stdout();
                output_sunrise_results_parquet(
                    results,
                    stdout,
                    show_inputs,
                    show_headers,
                    show_twilight,
                    is_stdin,
                )
            }
            #[cfg(not(feature = "parquet"))]
            {
                unreachable!(
                    "Parquet format should be rejected during parsing when feature is disabled"
                )
            }
        }
        _ => output_sunrise_results_buffered(
            results,
            &mut writer,
            format,
            show_inputs,
            show_headers,
            show_twilight,
            is_stdin,
        ),
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

fn output_sunrise_results_buffered<I>(
    results: I,
    writer: &mut BufWriter<io::StdoutLock>,
    format: &OutputFormat,
    show_inputs: bool,
    show_headers: bool,
    show_twilight: bool,
    flush_each: bool,
) -> io::Result<()>
where
    I: Iterator<Item = SunriseResultData>,
{
    let mut first = true;
    for result in results {
        if first && *format == OutputFormat::Csv && show_headers {
            print_csv_headers(writer, show_inputs, show_twilight)?;
            first = false;
        }
        match format {
            OutputFormat::Human => print_human(writer, &result, show_inputs, show_twilight)?,
            OutputFormat::Csv => print_csv(writer, &result, show_inputs, show_twilight)?,
            OutputFormat::Json => print_json(writer, &result, show_inputs, show_twilight)?,
            OutputFormat::Parquet => {
                // This should never be reached as parquet is handled at the top level
                unreachable!("Parquet format should be handled at the top level")
            }
        }
        if flush_each {
            writer.flush()?;
        }
    }
    writer.flush()?;
    Ok(())
}

fn print_human(
    writer: &mut BufWriter<io::StdoutLock>,
    result: &SunriseResultData,
    show_inputs: bool,
    show_twilight: bool,
) -> io::Result<()> {
    if show_inputs {
        writeln!(
            writer,
            "latitude :                     {:.5}°",
            result.latitude
        )?;
        writeln!(
            writer,
            "longitude:                     {:.5}°",
            result.longitude
        )?;
        writeln!(
            writer,
            "date/time: {} {}",
            result.datetime.format(DATE_FORMAT),
            result.datetime.format(TIME_FORMAT)
        )?;
        writeln!(
            writer,
            "delta T  :                        {:.3} s",
            result.delta_t
        )?;
    }

    // Type field
    let type_str = match &result.sunrise_result {
        SunriseResult::RegularDay { .. } => "normal",
        SunriseResult::AllDay { .. } => "all day",
        SunriseResult::AllNight { .. } => "all night",
    };

    if show_inputs {
        writeln!(writer, "type     : {}", type_str)?;
    } else {
        writeln!(writer, "type   : {}", type_str)?;
    }

    // Sunrise/transit/sunset fields
    match &result.sunrise_result {
        SunriseResult::RegularDay {
            sunrise,
            transit,
            sunset,
        } => {
            if show_inputs {
                writeln!(
                    writer,
                    "sunrise  : {} {}",
                    sunrise.format(DATE_FORMAT),
                    sunrise.format(TIME_FORMAT)
                )?;
                writeln!(
                    writer,
                    "transit  : {} {}",
                    transit.format(DATE_FORMAT),
                    transit.format(TIME_FORMAT)
                )?;
                writeln!(
                    writer,
                    "sunset   : {} {}",
                    sunset.format(DATE_FORMAT),
                    sunset.format(TIME_FORMAT)
                )?;
            } else {
                writeln!(
                    writer,
                    "sunrise: {} {}",
                    sunrise.format(DATE_FORMAT),
                    sunrise.format(TIME_FORMAT)
                )?;
                writeln!(
                    writer,
                    "transit: {} {}",
                    transit.format(DATE_FORMAT),
                    transit.format(TIME_FORMAT)
                )?;
                writeln!(
                    writer,
                    "sunset : {} {}",
                    sunset.format(DATE_FORMAT),
                    sunset.format(TIME_FORMAT)
                )?;
            }
        }
        SunriseResult::AllDay { transit } => {
            if show_inputs {
                writeln!(writer, "sunrise  : ")?;
                writeln!(
                    writer,
                    "transit  : {} {}",
                    transit.format(DATE_FORMAT),
                    transit.format(TIME_FORMAT)
                )?;
                writeln!(writer, "sunset   : ")?;
            } else {
                writeln!(writer, "sunrise: ")?;
                writeln!(
                    writer,
                    "transit: {} {}",
                    transit.format(DATE_FORMAT),
                    transit.format(TIME_FORMAT)
                )?;
                writeln!(writer, "sunset : ")?;
            }
        }
        SunriseResult::AllNight { transit } => {
            if show_inputs {
                writeln!(writer, "sunrise  : ")?;
                writeln!(
                    writer,
                    "transit  : {} {}",
                    transit.format(DATE_FORMAT),
                    transit.format(TIME_FORMAT)
                )?;
                writeln!(writer, "sunset   : ")?;
            } else {
                writeln!(writer, "sunrise: ")?;
                writeln!(
                    writer,
                    "transit: {} {}",
                    transit.format(DATE_FORMAT),
                    transit.format(TIME_FORMAT)
                )?;
                writeln!(writer, "sunset : ")?;
            }
        }
    }

    if show_twilight && let Some(twilight) = &result.twilight_results {
        print_twilight_human(writer, twilight)?;
    }
    writeln!(writer)?;
    Ok(())
}

fn print_twilight_human(
    writer: &mut BufWriter<io::StdoutLock>,
    twilight: &TwilightResults,
) -> io::Result<()> {
    // Print twilight starts in order: astronomical, nautical, civil
    match &twilight.astronomical {
        SunriseResult::RegularDay { sunrise, .. } => {
            writeln!(
                writer,
                "astronomical_start: {} {}",
                sunrise.format(DATE_FORMAT),
                sunrise.format(TIME_FORMAT)
            )?;
        }
        _ => writeln!(writer, "astronomical_start: ")?,
    }

    match &twilight.nautical {
        SunriseResult::RegularDay { sunrise, .. } => {
            writeln!(
                writer,
                "nautical_start    : {} {}",
                sunrise.format(DATE_FORMAT),
                sunrise.format(TIME_FORMAT)
            )?;
        }
        _ => writeln!(writer, "nautical_start    : ")?,
    }

    match &twilight.civil {
        SunriseResult::RegularDay { sunrise, .. } => {
            writeln!(
                writer,
                "civil_start       : {} {}",
                sunrise.format(DATE_FORMAT),
                sunrise.format(TIME_FORMAT)
            )?;
        }
        _ => writeln!(writer, "civil_start       : ")?,
    }

    // Print twilight ends in order: civil, nautical, astronomical
    match &twilight.civil {
        SunriseResult::RegularDay { sunset, .. } => {
            writeln!(
                writer,
                "civil_end         : {} {}",
                sunset.format(DATE_FORMAT),
                sunset.format(TIME_FORMAT)
            )?;
        }
        _ => writeln!(writer, "civil_end         : ")?,
    }

    match &twilight.nautical {
        SunriseResult::RegularDay { sunset, .. } => {
            writeln!(
                writer,
                "nautical_end      : {} {}",
                sunset.format(DATE_FORMAT),
                sunset.format(TIME_FORMAT)
            )?;
        }
        _ => writeln!(writer, "nautical_end      : ")?,
    }

    match &twilight.astronomical {
        SunriseResult::RegularDay { sunset, .. } => {
            writeln!(
                writer,
                "astronomical_end  : {} {}",
                sunset.format(DATE_FORMAT),
                sunset.format(TIME_FORMAT)
            )?;
        }
        _ => writeln!(writer, "astronomical_end  : ")?,
    }

    Ok(())
}

fn print_csv_headers(
    writer: &mut BufWriter<io::StdoutLock>,
    show_inputs: bool,
    show_twilight: bool,
) -> io::Result<()> {
    if show_inputs {
        write!(writer, "latitude,longitude,dateTime,deltaT,")?;
    }
    write!(writer, "type,sunrise,transit,sunset")?;

    if show_twilight {
        write!(
            writer,
            ",civil_start,civil_end,nautical_start,nautical_end,astronomical_start,astronomical_end"
        )?;
    }
    writeln!(writer)?;
    Ok(())
}

fn print_csv(
    writer: &mut BufWriter<io::StdoutLock>,
    result: &SunriseResultData,
    show_inputs: bool,
    show_twilight: bool,
) -> io::Result<()> {
    if show_inputs {
        write!(
            writer,
            "{:.5},{:.5},{},{:.1},",
            result.latitude,
            result.longitude,
            format_datetime_solarpos(&result.datetime),
            result.delta_t
        )?;
    }

    // Print type first
    let result_type = match &result.sunrise_result {
        SunriseResult::RegularDay { .. } => "NORMAL",
        SunriseResult::AllDay { .. } => "ALL_DAY",
        SunriseResult::AllNight { .. } => "ALL_NIGHT",
    };
    write!(writer, "{},", result_type)?;

    match &result.sunrise_result {
        SunriseResult::RegularDay {
            sunrise,
            transit,
            sunset,
        } => {
            write!(
                writer,
                "{},{},{}",
                format_datetime_solarpos(sunrise),
                format_datetime_solarpos(transit),
                format_datetime_solarpos(sunset)
            )?;
        }
        SunriseResult::AllDay { transit } => {
            write!(writer, ",{},", format_datetime_solarpos(transit))?;
        }
        SunriseResult::AllNight { transit } => {
            write!(writer, ",{},", format_datetime_solarpos(transit))?;
        }
    }

    if show_twilight {
        if let Some(twilight) = &result.twilight_results {
            print_twilight_csv(writer, twilight)?;
        } else {
            write!(writer, ",,,,,,")?;
        }
    }
    writeln!(writer)?;
    Ok(())
}

fn print_twilight_csv(
    writer: &mut BufWriter<io::StdoutLock>,
    twilight: &TwilightResults,
) -> io::Result<()> {
    for result in [&twilight.civil, &twilight.nautical, &twilight.astronomical] {
        match result {
            SunriseResult::RegularDay {
                sunrise, sunset, ..
            } => {
                write!(
                    writer,
                    ",{},{}",
                    format_datetime_solarpos(sunrise),
                    format_datetime_solarpos(sunset)
                )?;
            }
            SunriseResult::AllDay { .. } => write!(writer, ",,")?,
            SunriseResult::AllNight { .. } => write!(writer, ",,")?,
        }
    }
    Ok(())
}

fn print_json(
    writer: &mut BufWriter<io::StdoutLock>,
    result: &SunriseResultData,
    show_inputs: bool,
    show_twilight: bool,
) -> io::Result<()> {
    write!(writer, "{{")?;

    // Only include input fields when show_inputs is true
    if show_inputs {
        write!(
            writer,
            "\"latitude\":{:.5},\"longitude\":{:.5},\"dateTime\":\"{}\",\"deltaT\":{:.3},",
            result.latitude,
            result.longitude,
            format_datetime_solarpos(&result.datetime),
            result.delta_t
        )?;
    }

    match &result.sunrise_result {
        SunriseResult::RegularDay {
            sunrise,
            transit,
            sunset,
        } => {
            write!(
                writer,
                "\"type\":\"NORMAL\",\"sunrise\":\"{}\",\"transit\":\"{}\",\"sunset\":\"{}\"",
                format_datetime_solarpos(sunrise),
                format_datetime_solarpos(transit),
                format_datetime_solarpos(sunset)
            )?;
        }
        SunriseResult::AllDay { transit } => {
            write!(
                writer,
                "\"type\":\"ALL_DAY\",\"sunrise\":null,\"transit\":\"{}\",\"sunset\":null",
                format_datetime_solarpos(transit)
            )?;
        }
        SunriseResult::AllNight { transit } => {
            write!(
                writer,
                "\"type\":\"ALL_NIGHT\",\"sunrise\":null,\"transit\":\"{}\",\"sunset\":null",
                format_datetime_solarpos(transit)
            )?;
        }
    }

    if show_twilight && let Some(twilight) = &result.twilight_results {
        print_twilight_json(writer, twilight)?;
    }
    writeln!(writer, "}}")?;
    Ok(())
}

fn print_twilight_json(
    writer: &mut BufWriter<io::StdoutLock>,
    twilight: &TwilightResults,
) -> io::Result<()> {
    for (name, result) in [
        ("civil", &twilight.civil),
        ("nautical", &twilight.nautical),
        ("astronomical", &twilight.astronomical),
    ] {
        match result {
            SunriseResult::RegularDay {
                sunrise, sunset, ..
            } => {
                write!(
                    writer,
                    ",\"{}_start\":\"{}\",\"{}_end\":\"{}\"",
                    name,
                    format_datetime_solarpos(sunrise),
                    name,
                    format_datetime_solarpos(sunset)
                )?;
            }
            SunriseResult::AllDay { .. } => {
                write!(writer, ",\"{}_start\":null,\"{}_end\":null", name, name)?;
            }
            SunriseResult::AllNight { .. } => {
                write!(writer, ",\"{}_start\":null,\"{}_end\":null", name, name)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use solar_positioning::types::SunriseResult;

    #[test]
    fn test_sunrise_result_data_creation() {
        let tz = FixedOffset::east_opt(0).unwrap();
        let dt = tz.with_ymd_and_hms(2024, 6, 21, 12, 0, 0).unwrap();
        let sunrise = tz.with_ymd_and_hms(2024, 6, 21, 5, 30, 0).unwrap();
        let noon = tz.with_ymd_and_hms(2024, 6, 21, 12, 0, 0).unwrap();
        let sunset = tz.with_ymd_and_hms(2024, 6, 21, 18, 30, 0).unwrap();

        let data = SunriseResultData {
            datetime: dt,
            latitude: 52.0,
            longitude: 13.4,
            delta_t: 69.2,
            sunrise_result: SunriseResult::RegularDay {
                sunrise,
                transit: noon,
                sunset,
            },
            twilight_results: None,
        };

        assert_eq!(data.latitude, 52.0);
        assert_eq!(data.longitude, 13.4);
        assert_eq!(data.delta_t, 69.2);
    }

    #[test]
    fn test_twilight_results_creation() {
        let tz = FixedOffset::east_opt(0).unwrap();
        let sunrise = tz.with_ymd_and_hms(2024, 6, 21, 5, 30, 0).unwrap();
        let noon = tz.with_ymd_and_hms(2024, 6, 21, 12, 0, 0).unwrap();
        let sunset = tz.with_ymd_and_hms(2024, 6, 21, 18, 30, 0).unwrap();

        let twilight = TwilightResults {
            civil: SunriseResult::RegularDay {
                sunrise,
                transit: noon,
                sunset,
            },
            nautical: SunriseResult::RegularDay {
                sunrise,
                transit: noon,
                sunset,
            },
            astronomical: SunriseResult::RegularDay {
                sunrise,
                transit: noon,
                sunset,
            },
        };

        match twilight.civil {
            SunriseResult::RegularDay { .. } => {}
            _ => panic!("Expected regular day"),
        }
    }
}
