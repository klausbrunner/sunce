use chrono::{DateTime, FixedOffset};
use solar_positioning::types::SunriseResult;

#[derive(Debug, Clone, PartialEq)]
pub enum OutputFormat {
    Human,
    Csv,
    Json,
}

fn format_datetime_solarpos(dt: &DateTime<FixedOffset>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S%:z").to_string()
}

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
    let mut first = true;
    for result in results {
        if first && *format == OutputFormat::Csv && show_headers {
            print_csv_headers(show_inputs, show_twilight);
            first = false;
        }
        match format {
            OutputFormat::Human => print_human(&result, show_twilight),
            OutputFormat::Csv => print_csv(&result, show_inputs, show_twilight),
            OutputFormat::Json => print_json(&result, show_twilight),
        }
    }
}

fn print_human(result: &SunriseResultData, show_twilight: bool) {
    println!(
        "latitude          :                     {:.5}°",
        result.latitude
    );
    println!(
        "longitude         :                     {:.5}°",
        result.longitude
    );
    println!(
        "datetime          :    {}",
        format_datetime_solarpos(&result.datetime)
    );
    println!(
        "delta T           :                   {:.1} s",
        result.delta_t
    );

    match &result.sunrise_result {
        SunriseResult::RegularDay {
            sunrise,
            transit,
            sunset,
        } => {
            println!(
                "sunrise           :    {}",
                format_datetime_solarpos(sunrise)
            );
            println!(
                "solar noon        :    {}",
                format_datetime_solarpos(transit)
            );
            println!(
                "sunset            :    {}",
                format_datetime_solarpos(sunset)
            );
        }
        SunriseResult::AllDay { transit } => {
            println!(
                "polar day         :    {}",
                format_datetime_solarpos(transit)
            );
        }
        SunriseResult::AllNight { transit } => {
            println!(
                "polar night       :    {}",
                format_datetime_solarpos(transit)
            );
        }
    }

    if show_twilight {
        if let Some(twilight) = &result.twilight_results {
            print_twilight_human(twilight);
        }
    }
    println!();
}

fn print_twilight_human(twilight: &TwilightResults) {
    for (name, result) in [
        ("civil", &twilight.civil),
        ("nautical", &twilight.nautical),
        ("astronomical", &twilight.astronomical),
    ] {
        match result {
            SunriseResult::RegularDay {
                sunrise, sunset, ..
            } => {
                println!(
                    "{} dawn        :    {}",
                    name,
                    format_datetime_solarpos(sunrise)
                );
                println!(
                    "{} dusk        :    {}",
                    name,
                    format_datetime_solarpos(sunset)
                );
            }
            SunriseResult::AllDay { .. } => {
                println!("{} twilight    :    polar day", name);
            }
            SunriseResult::AllNight { .. } => {
                println!("{} twilight    :    polar night", name);
            }
        }
    }
}

fn print_csv_headers(show_inputs: bool, show_twilight: bool) {
    if show_inputs {
        print!("latitude,longitude,dateTime,deltaT,");
    }
    print!("type,sunrise,transit,sunset");

    if show_twilight {
        print!(
            ",civil_dawn,civil_dusk,nautical_dawn,nautical_dusk,astronomical_dawn,astronomical_dusk"
        );
    }
    println!();
}

fn print_csv(result: &SunriseResultData, show_inputs: bool, show_twilight: bool) {
    if show_inputs {
        print!(
            "{:.5},{:.5},{},{:.1},",
            result.latitude,
            result.longitude,
            format_datetime_solarpos(&result.datetime),
            result.delta_t
        );
    }

    // Print type first
    let result_type = match &result.sunrise_result {
        SunriseResult::RegularDay { .. } => "normal",
        SunriseResult::AllDay { .. } => "polar_day",
        SunriseResult::AllNight { .. } => "polar_night",
    };
    print!("{},", result_type);

    match &result.sunrise_result {
        SunriseResult::RegularDay {
            sunrise,
            transit,
            sunset,
        } => {
            print!(
                "{},{},{}",
                format_datetime_solarpos(sunrise),
                format_datetime_solarpos(transit),
                format_datetime_solarpos(sunset)
            );
        }
        SunriseResult::AllDay { transit } => {
            print!("polar_day,{},polar_day", format_datetime_solarpos(transit));
        }
        SunriseResult::AllNight { transit } => {
            print!(
                "polar_night,{},polar_night",
                format_datetime_solarpos(transit)
            );
        }
    }

    if show_twilight {
        if let Some(twilight) = &result.twilight_results {
            print_twilight_csv(twilight);
        } else {
            print!(",,,,,,");
        }
    }
    println!();
}

fn print_twilight_csv(twilight: &TwilightResults) {
    for result in [&twilight.civil, &twilight.nautical, &twilight.astronomical] {
        match result {
            SunriseResult::RegularDay {
                sunrise, sunset, ..
            } => {
                print!(
                    ",{},{}",
                    format_datetime_solarpos(sunrise),
                    format_datetime_solarpos(sunset)
                );
            }
            SunriseResult::AllDay { .. } => print!(",polar_day,polar_day"),
            SunriseResult::AllNight { .. } => print!(",polar_night,polar_night"),
        }
    }
}

fn print_json(result: &SunriseResultData, show_twilight: bool) {
    print!(
        "{{\"latitude\":{:.5},\"longitude\":{:.5},\"dateTime\":\"{}\",\"deltaT\":{:.3}",
        result.latitude,
        result.longitude,
        format_datetime_solarpos(&result.datetime),
        result.delta_t
    );

    match &result.sunrise_result {
        SunriseResult::RegularDay {
            sunrise,
            transit,
            sunset,
        } => {
            print!(
                ",\"type\":\"NORMAL\",\"sunrise\":\"{}\",\"transit\":\"{}\",\"sunset\":\"{}\"",
                format_datetime_solarpos(sunrise),
                format_datetime_solarpos(transit),
                format_datetime_solarpos(sunset)
            );
        }
        SunriseResult::AllDay { transit } => {
            print!(
                ",\"type\":\"POLAR_DAY\",\"sunrise\":\"polar_day\",\"transit\":\"{}\",\"sunset\":\"polar_day\"",
                format_datetime_solarpos(transit)
            );
        }
        SunriseResult::AllNight { transit } => {
            print!(
                ",\"type\":\"POLAR_NIGHT\",\"sunrise\":\"polar_night\",\"transit\":\"{}\",\"sunset\":\"polar_night\"",
                format_datetime_solarpos(transit)
            );
        }
    }

    if show_twilight {
        if let Some(twilight) = &result.twilight_results {
            print_twilight_json(twilight);
        }
    }
    println!("}}");
}

fn print_twilight_json(twilight: &TwilightResults) {
    for (name, result) in [
        ("civil", &twilight.civil),
        ("nautical", &twilight.nautical),
        ("astronomical", &twilight.astronomical),
    ] {
        match result {
            SunriseResult::RegularDay {
                sunrise, sunset, ..
            } => {
                print!(
                    ",\"{}_dawn\":\"{}\",\"{}_dusk\":\"{}\"",
                    name,
                    format_datetime_solarpos(sunrise),
                    name,
                    format_datetime_solarpos(sunset)
                );
            }
            SunriseResult::AllDay { .. } => {
                print!(
                    ",\"{}_dawn\":\"polar_day\",\"{}_dusk\":\"polar_day\"",
                    name, name
                );
            }
            SunriseResult::AllNight { .. } => {
                print!(
                    ",\"{}_dawn\":\"polar_night\",\"{}_dusk\":\"polar_night\"",
                    name, name
                );
            }
        }
    }
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
