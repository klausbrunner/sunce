use crate::types::{OutputFormat, format_datetime_solarpos};
use chrono::{DateTime, FixedOffset};
use solar_positioning::types::SunriseResult;
use std::io::{self, BufWriter, Write};

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
) where
    I: Iterator<Item = SunriseResultData>,
{
    let stdout = io::stdout().lock();
    let mut writer = BufWriter::with_capacity(1024, stdout);

    let result = output_sunrise_results_buffered(
        results,
        &mut writer,
        format,
        show_inputs,
        show_headers,
        show_twilight,
    );

    if let Err(e) = result {
        eprintln!("✗ Output error: {}", e);
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
            OutputFormat::Human => print_human(writer, &result, show_twilight)?,
            OutputFormat::Csv => print_csv(writer, &result, show_inputs, show_twilight)?,
            OutputFormat::Json => print_json(writer, &result, show_twilight)?,
        }
    }
    writer.flush()?;
    Ok(())
}

fn print_human(
    writer: &mut BufWriter<io::StdoutLock>,
    result: &SunriseResultData,
    show_twilight: bool,
) -> io::Result<()> {
    writeln!(
        writer,
        "latitude          :                     {:.5}°",
        result.latitude
    )?;
    writeln!(
        writer,
        "longitude         :                     {:.5}°",
        result.longitude
    )?;
    writeln!(
        writer,
        "datetime          :    {}",
        format_datetime_solarpos(&result.datetime)
    )?;
    writeln!(
        writer,
        "delta T           :                   {:.1} s",
        result.delta_t
    )?;

    match &result.sunrise_result {
        SunriseResult::RegularDay {
            sunrise,
            transit,
            sunset,
        } => {
            writeln!(
                writer,
                "sunrise           :    {}",
                format_datetime_solarpos(sunrise)
            )?;
            writeln!(
                writer,
                "solar noon        :    {}",
                format_datetime_solarpos(transit)
            )?;
            writeln!(
                writer,
                "sunset            :    {}",
                format_datetime_solarpos(sunset)
            )?;
        }
        SunriseResult::AllDay { transit } => {
            writeln!(
                writer,
                "polar day         :    {}",
                format_datetime_solarpos(transit)
            )?;
        }
        SunriseResult::AllNight { transit } => {
            writeln!(
                writer,
                "polar night       :    {}",
                format_datetime_solarpos(transit)
            )?;
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
    for (name, result) in [
        ("civil", &twilight.civil),
        ("nautical", &twilight.nautical),
        ("astronomical", &twilight.astronomical),
    ] {
        match result {
            SunriseResult::RegularDay {
                sunrise, sunset, ..
            } => {
                writeln!(
                    writer,
                    "{} dawn        :    {}",
                    name,
                    format_datetime_solarpos(sunrise)
                )?;
                writeln!(
                    writer,
                    "{} dusk        :    {}",
                    name,
                    format_datetime_solarpos(sunset)
                )?;
            }
            SunriseResult::AllDay { .. } => {
                writeln!(writer, "{} twilight    :    polar day", name)?;
            }
            SunriseResult::AllNight { .. } => {
                writeln!(writer, "{} twilight    :    polar night", name)?;
            }
        }
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
            ",civil_dawn,civil_dusk,nautical_dawn,nautical_dusk,astronomical_dawn,astronomical_dusk"
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
        SunriseResult::RegularDay { .. } => "normal",
        SunriseResult::AllDay { .. } => "polar_day",
        SunriseResult::AllNight { .. } => "polar_night",
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
            write!(
                writer,
                "polar_day,{},polar_day",
                format_datetime_solarpos(transit)
            )?;
        }
        SunriseResult::AllNight { transit } => {
            write!(
                writer,
                "polar_night,{},polar_night",
                format_datetime_solarpos(transit)
            )?;
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
            SunriseResult::AllDay { .. } => write!(writer, ",polar_day,polar_day")?,
            SunriseResult::AllNight { .. } => write!(writer, ",polar_night,polar_night")?,
        }
    }
    Ok(())
}

fn print_json(
    writer: &mut BufWriter<io::StdoutLock>,
    result: &SunriseResultData,
    show_twilight: bool,
) -> io::Result<()> {
    write!(
        writer,
        "{{\"latitude\":{:.5},\"longitude\":{:.5},\"dateTime\":\"{}\",\"deltaT\":{:.3}",
        result.latitude,
        result.longitude,
        format_datetime_solarpos(&result.datetime),
        result.delta_t
    )?;

    match &result.sunrise_result {
        SunriseResult::RegularDay {
            sunrise,
            transit,
            sunset,
        } => {
            write!(
                writer,
                ",\"type\":\"NORMAL\",\"sunrise\":\"{}\",\"transit\":\"{}\",\"sunset\":\"{}\"",
                format_datetime_solarpos(sunrise),
                format_datetime_solarpos(transit),
                format_datetime_solarpos(sunset)
            )?;
        }
        SunriseResult::AllDay { transit } => {
            write!(
                writer,
                ",\"type\":\"POLAR_DAY\",\"sunrise\":\"polar_day\",\"transit\":\"{}\",\"sunset\":\"polar_day\"",
                format_datetime_solarpos(transit)
            )?;
        }
        SunriseResult::AllNight { transit } => {
            write!(
                writer,
                ",\"type\":\"POLAR_NIGHT\",\"sunrise\":\"polar_night\",\"transit\":\"{}\",\"sunset\":\"polar_night\"",
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
                    ",\"{}_dawn\":\"{}\",\"{}_dusk\":\"{}\"",
                    name,
                    format_datetime_solarpos(sunrise),
                    name,
                    format_datetime_solarpos(sunset)
                )?;
            }
            SunriseResult::AllDay { .. } => {
                write!(
                    writer,
                    ",\"{}_dawn\":\"polar_day\",\"{}_dusk\":\"polar_day\"",
                    name, name
                )?;
            }
            SunriseResult::AllNight { .. } => {
                write!(
                    writer,
                    ",\"{}_dawn\":\"polar_night\",\"{}_dusk\":\"polar_night\"",
                    name, name
                )?;
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
