use chrono::{DateTime, FixedOffset};
use solar_positioning::types::SunriseResult;

use crate::output::OutputFormat;

/// Format datetime to match solarpos format (no subseconds)
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

impl SunriseResultData {
    pub fn new(
        datetime: DateTime<FixedOffset>,
        latitude: f64,
        longitude: f64,
        delta_t: f64,
        sunrise_result: SunriseResult<DateTime<FixedOffset>>,
        twilight_results: Option<TwilightResults>,
    ) -> Self {
        Self {
            datetime,
            latitude,
            longitude,
            delta_t,
            sunrise_result,
            twilight_results,
        }
    }
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
    match format {
        OutputFormat::Human => output_sunrise_human_format(results, show_inputs, show_twilight),
        OutputFormat::Csv => {
            output_sunrise_csv_format(results, show_inputs, show_headers, show_twilight)
        }
        OutputFormat::Json => output_sunrise_json_format(results, show_inputs, show_twilight),
    }
}

fn output_sunrise_human_format<I>(results: I, _show_inputs: bool, show_twilight: bool)
where
    I: Iterator<Item = SunriseResultData>,
{
    for result in results {
        println!(
            "latitude          :                     {:.5}°",
            result.latitude
        );
        println!(
            "longitude         :                     {:.5}°",
            result.longitude
        );
        println!(
            "date/time         : {}",
            result.datetime.format("%Y-%m-%d %H:%M:%S%:z")
        );
        println!(
            "delta T           :                        {:.3} s",
            result.delta_t
        );

        match &result.sunrise_result {
            SunriseResult::RegularDay {
                sunrise,
                transit,
                sunset,
            } => {
                println!("type              : normal");
                if show_twilight {
                    if let Some(twilight) = &result.twilight_results {
                        print_twilight_times_human(twilight, true);
                    }
                }
                println!(
                    "sunrise           : {}",
                    sunrise.format("%Y-%m-%d %H:%M:%S%:z")
                );
                println!(
                    "transit           : {}",
                    transit.format("%Y-%m-%d %H:%M:%S%:z")
                );
                println!(
                    "sunset            : {}",
                    sunset.format("%Y-%m-%d %H:%M:%S%:z")
                );
                if show_twilight {
                    if let Some(twilight) = &result.twilight_results {
                        print_twilight_times_human(twilight, false);
                    }
                }
            }
            SunriseResult::AllDay { transit } => {
                println!("type              : all day");
                println!("sunrise           : ");
                println!(
                    "transit           : {}",
                    transit.format("%Y-%m-%d %H:%M:%S%:z")
                );
                println!("sunset            : ");
            }
            SunriseResult::AllNight { transit } => {
                println!("type              : all night");
                println!("sunrise           : ");
                println!(
                    "transit           : {}",
                    transit.format("%Y-%m-%d %H:%M:%S%:z")
                );
                println!("sunset            : ");
            }
        }
        println!();
    }
}

fn print_twilight_times_human(twilight: &TwilightResults, is_start: bool) {
    if is_start {
        // Print start times (before sunrise) - in reverse chronological order
        if let SunriseResult::RegularDay {
            sunrise: astronomical_start,
            ..
        } = &twilight.astronomical
        {
            println!(
                "astronomical_start: {}",
                astronomical_start.format("%Y-%m-%d %H:%M:%S%:z")
            );
        }
        if let SunriseResult::RegularDay {
            sunrise: nautical_start,
            ..
        } = &twilight.nautical
        {
            println!(
                "nautical_start    : {}",
                nautical_start.format("%Y-%m-%d %H:%M:%S%:z")
            );
        }
        if let SunriseResult::RegularDay {
            sunrise: civil_start,
            ..
        } = &twilight.civil
        {
            println!(
                "civil_start       : {}",
                civil_start.format("%Y-%m-%d %H:%M:%S%:z")
            );
        }
    } else {
        // Print end times (after sunset) - in chronological order
        if let SunriseResult::RegularDay {
            sunset: civil_end, ..
        } = &twilight.civil
        {
            println!(
                "civil_end         : {}",
                civil_end.format("%Y-%m-%d %H:%M:%S%:z")
            );
        }
        if let SunriseResult::RegularDay {
            sunset: nautical_end,
            ..
        } = &twilight.nautical
        {
            println!(
                "nautical_end      : {}",
                nautical_end.format("%Y-%m-%d %H:%M:%S%:z")
            );
        }
        if let SunriseResult::RegularDay {
            sunset: astronomical_end,
            ..
        } = &twilight.astronomical
        {
            println!(
                "astronomical_end  : {}",
                astronomical_end.format("%Y-%m-%d %H:%M:%S%:z")
            );
        }
    }
}

fn output_sunrise_csv_format<I>(
    results: I,
    _show_inputs: bool,
    show_headers: bool,
    show_twilight: bool,
) where
    I: Iterator<Item = SunriseResultData>,
{
    if show_headers {
        if show_twilight {
            println!(
                "latitude,longitude,dateTime,deltaT,type,sunrise,transit,sunset,civil_start,civil_end,nautical_start,nautical_end,astronomical_start,astronomical_end"
            );
        } else {
            println!("latitude,longitude,dateTime,deltaT,type,sunrise,transit,sunset");
        }
    }

    for result in results {
        print!(
            "{:.5},{:.5},{},{:.3},",
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
                    "NORMAL,{},{},{}",
                    format_datetime_solarpos(sunrise),
                    format_datetime_solarpos(transit),
                    format_datetime_solarpos(sunset)
                );

                if show_twilight {
                    if let Some(twilight) = &result.twilight_results {
                        print_twilight_times_csv(twilight);
                    }
                }
            }
            SunriseResult::AllDay { transit } => {
                print!("ALL_DAY,,{},", transit.to_rfc3339());
                if show_twilight {
                    print!(",,,,,,");
                }
            }
            SunriseResult::AllNight { transit } => {
                print!("ALL_NIGHT,,{},", transit.to_rfc3339());
                if show_twilight {
                    print!(",,,,,,");
                }
            }
        }
        println!();
    }
}

fn print_twilight_times_csv(twilight: &TwilightResults) {
    // Extract times in the order: civil_start, civil_end, nautical_start, nautical_end, astronomical_start, astronomical_end
    let civil_start = if let SunriseResult::RegularDay { sunrise, .. } = &twilight.civil {
        sunrise.to_rfc3339()
    } else {
        String::new()
    };

    let civil_end = if let SunriseResult::RegularDay { sunset, .. } = &twilight.civil {
        sunset.to_rfc3339()
    } else {
        String::new()
    };

    let nautical_start = if let SunriseResult::RegularDay { sunrise, .. } = &twilight.nautical {
        sunrise.to_rfc3339()
    } else {
        String::new()
    };

    let nautical_end = if let SunriseResult::RegularDay { sunset, .. } = &twilight.nautical {
        sunset.to_rfc3339()
    } else {
        String::new()
    };

    let astronomical_start =
        if let SunriseResult::RegularDay { sunrise, .. } = &twilight.astronomical {
            sunrise.to_rfc3339()
        } else {
            String::new()
        };

    let astronomical_end = if let SunriseResult::RegularDay { sunset, .. } = &twilight.astronomical
    {
        sunset.to_rfc3339()
    } else {
        String::new()
    };

    print!(
        ",{},{},{},{},{},{}",
        civil_start, civil_end, nautical_start, nautical_end, astronomical_start, astronomical_end
    );
}

fn output_sunrise_json_format<I>(results: I, _show_inputs: bool, show_twilight: bool)
where
    I: Iterator<Item = SunriseResultData>,
{
    for result in results {
        print!(
            r#"{{"latitude":{:.5},"longitude":{:.5},"dateTime":"{}","deltaT":{:.3},"#,
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
                    r#""type":"NORMAL","sunrise":"{}","transit":"{}","sunset":"{}""#,
                    format_datetime_solarpos(sunrise),
                    format_datetime_solarpos(transit),
                    format_datetime_solarpos(sunset)
                );

                if show_twilight {
                    if let Some(twilight) = &result.twilight_results {
                        print_twilight_times_json(twilight);
                    }
                }
            }
            SunriseResult::AllDay { transit } => {
                print!(
                    r#""type":"ALL_DAY","sunrise":"","transit":"{}","sunset":"#,
                    transit.to_rfc3339()
                );
            }
            SunriseResult::AllNight { transit } => {
                print!(
                    r#""type":"ALL_NIGHT","sunrise":"","transit":"{}","sunset":"#,
                    transit.to_rfc3339()
                );
            }
        }
        println!("}}");
    }
}

fn print_twilight_times_json(twilight: &TwilightResults) {
    if let SunriseResult::RegularDay {
        sunrise: civil_start,
        sunset: civil_end,
        ..
    } = &twilight.civil
    {
        print!(
            r#","civil_start":"{}","civil_end":"{}""#,
            civil_start.to_rfc3339(),
            civil_end.to_rfc3339()
        );
    }
    if let SunriseResult::RegularDay {
        sunrise: nautical_start,
        sunset: nautical_end,
        ..
    } = &twilight.nautical
    {
        print!(
            r#","nautical_start":"{}","nautical_end":"{}""#,
            nautical_start.to_rfc3339(),
            nautical_end.to_rfc3339()
        );
    }
    if let SunriseResult::RegularDay {
        sunrise: astronomical_start,
        sunset: astronomical_end,
        ..
    } = &twilight.astronomical
    {
        print!(
            r#","astronomical_start":"{}","astronomical_end":"{}""#,
            astronomical_start.to_rfc3339(),
            astronomical_end.to_rfc3339()
        );
    }
}
