use crate::output::PositionResult;
use std::io::{self, Write};

#[derive(Debug, Clone, Default)]
pub struct VarianceFlags {
    pub latitude: bool,
    pub longitude: bool,
    pub elevation: bool,
    pub pressure: bool,
    pub temperature: bool,
    pub datetime: bool,
    pub delta_t: bool,
}

impl VarianceFlags {
    pub fn detect(first: &PositionResult, second: &PositionResult) -> Self {
        Self {
            latitude: (first.latitude - second.latitude).abs() > f64::EPSILON,
            longitude: (first.longitude - second.longitude).abs() > f64::EPSILON,
            elevation: (first.elevation - second.elevation).abs() > f64::EPSILON,
            pressure: (first.pressure - second.pressure).abs() > f64::EPSILON,
            temperature: (first.temperature - second.temperature).abs() > f64::EPSILON,
            datetime: first.datetime != second.datetime,
            delta_t: (first.delta_t - second.delta_t).abs() > f64::EPSILON,
        }
    }
}

pub fn write_header_section<W: Write>(
    writer: &mut W,
    result: &PositionResult,
    variance: &VarianceFlags,
) -> io::Result<()> {
    let mut wrote_any = false;

    if !variance.latitude {
        writeln!(writer, "  Latitude:    {:.6}°", result.latitude)?;
        wrote_any = true;
    }
    if !variance.longitude {
        writeln!(writer, "  Longitude:   {:.6}°", result.longitude)?;
        wrote_any = true;
    }
    if !variance.elevation {
        writeln!(writer, "  Elevation:   {:.1} m", result.elevation)?;
        wrote_any = true;
    }
    if result.apply_refraction {
        if !variance.pressure {
            writeln!(writer, "  Pressure:    {:.1} hPa", result.pressure)?;
            wrote_any = true;
        }
        if !variance.temperature {
            writeln!(writer, "  Temperature: {:.1}°C", result.temperature)?;
            wrote_any = true;
        }
    }
    if !variance.datetime {
        writeln!(
            writer,
            "  DateTime:    {}",
            result.datetime.format("%Y-%m-%d %H:%M:%S%:z")
        )?;
        wrote_any = true;
    }
    if !variance.delta_t {
        writeln!(writer, "  Delta T:     {:.1} s", result.delta_t)?;
        wrote_any = true;
    }

    if wrote_any {
        writeln!(writer)?;
    }

    Ok(())
}

pub struct TableFormatter {
    pub variance: VarianceFlags,
    pub elevation_angle: bool,
    pub apply_refraction: bool,
}

impl TableFormatter {
    pub fn column_headers(&self) -> Vec<&str> {
        let mut headers = Vec::new();

        if self.variance.datetime {
            headers.push("DateTime");
        }
        if self.variance.latitude {
            headers.push("Latitude");
        }
        if self.variance.longitude {
            headers.push("Longitude");
        }
        if self.variance.elevation {
            headers.push("Elevation");
        }
        if self.apply_refraction && self.variance.pressure {
            headers.push("Pressure");
        }
        if self.apply_refraction && self.variance.temperature {
            headers.push("Temperature");
        }
        if self.variance.delta_t {
            headers.push("Delta T");
        }

        headers.push("Azimuth");
        if self.elevation_angle {
            headers.push("Elevation");
        } else {
            headers.push("Zenith");
        }

        headers
    }

    fn format_row(&self, result: &PositionResult) -> Vec<String> {
        let mut cells = Vec::new();

        if self.variance.datetime {
            cells.push(format!(
                "{}",
                result.datetime.format("%Y-%m-%d %H:%M:%S%:z")
            ));
        }
        if self.variance.latitude {
            cells.push(format!("{:>9.5}°", result.latitude));
        }
        if self.variance.longitude {
            cells.push(format!("{:>10.5}°", result.longitude));
        }
        if self.variance.elevation {
            cells.push(format!("{:>9.1} m", result.elevation));
        }
        if self.apply_refraction && self.variance.pressure {
            cells.push(format!("{:>8.1} hPa", result.pressure));
        }
        if self.apply_refraction && self.variance.temperature {
            cells.push(format!("{:>7.1}°C", result.temperature));
        }
        if self.variance.delta_t {
            cells.push(format!("{:>7.1} s", result.delta_t));
        }

        cells.push(format!("{:>9.5}°", result.position.azimuth()));

        let angle = if self.elevation_angle {
            result.position.elevation_angle()
        } else {
            result.position.zenith_angle()
        };
        cells.push(format!("{:>9.5}°", angle));

        cells
    }

    pub fn calculate_column_widths(&self, headers: &[&str]) -> Vec<usize> {
        headers
            .iter()
            .map(|h| {
                let base = h.len().max(12);
                if *h == "DateTime" { base.max(25) } else { base }
            })
            .collect()
    }

    pub fn write_table_header<W: Write>(
        &self,
        writer: &mut W,
        headers: &[&str],
        widths: &[usize],
    ) -> io::Result<()> {
        write!(writer, "┌")?;
        for (i, &width) in widths.iter().enumerate() {
            write!(writer, "{}", "─".repeat(width + 2))?;
            if i < widths.len() - 1 {
                write!(writer, "┬")?;
            }
        }
        writeln!(writer, "┐")?;

        write!(writer, "│")?;
        for (&header, &width) in headers.iter().zip(widths.iter()) {
            write!(writer, " {:<width$} ", header, width = width)?;
            write!(writer, "│")?;
        }
        writeln!(writer)?;

        write!(writer, "├")?;
        for (i, &width) in widths.iter().enumerate() {
            write!(writer, "{}", "─".repeat(width + 2))?;
            if i < widths.len() - 1 {
                write!(writer, "┼")?;
            }
        }
        writeln!(writer, "┤")?;

        Ok(())
    }

    pub fn write_table_row<W: Write>(
        &self,
        writer: &mut W,
        result: &PositionResult,
        widths: &[usize],
    ) -> io::Result<()> {
        let cells = self.format_row(result);

        write!(writer, "│")?;
        for (&width, cell) in widths.iter().zip(cells.iter()) {
            write!(writer, " {:>width$} ", cell, width = width)?;
            write!(writer, "│")?;
        }
        writeln!(writer)?;

        Ok(())
    }

    pub fn write_table_footer<W: Write>(&self, writer: &mut W, widths: &[usize]) -> io::Result<()> {
        write!(writer, "└")?;
        for (i, &width) in widths.iter().enumerate() {
            write!(writer, "{}", "─".repeat(width + 2))?;
            if i < widths.len() - 1 {
                write!(writer, "┴")?;
            }
        }
        writeln!(writer, "┘")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{FixedOffset, NaiveDate, TimeZone};
    use solar_positioning::types::SolarPosition;

    fn create_test_result(
        lat: f64,
        lon: f64,
        hour: u32,
        azimuth: f64,
        zenith: f64,
    ) -> PositionResult {
        let tz = FixedOffset::east_opt(7200).unwrap();
        let datetime = tz
            .from_local_datetime(
                &NaiveDate::from_ymd_opt(2024, 6, 21)
                    .unwrap()
                    .and_hms_opt(hour, 0, 0)
                    .unwrap(),
            )
            .unwrap();

        PositionResult {
            datetime,
            position: SolarPosition::new(azimuth, zenith).unwrap(),
            latitude: lat,
            longitude: lon,
            elevation: 0.0,
            pressure: 1013.0,
            temperature: 15.0,
            delta_t: 69.2,
            apply_refraction: true,
        }
    }

    #[test]
    fn test_variance_detection_no_variance() {
        let r1 = create_test_result(52.0, 13.0, 12, 180.0, 30.0);
        let r2 = create_test_result(52.0, 13.0, 12, 180.0, 30.0);

        let variance = VarianceFlags::detect(&r1, &r2);

        assert!(!variance.latitude);
        assert!(!variance.longitude);
        assert!(!variance.datetime);
    }

    #[test]
    fn test_variance_detection_time_series() {
        let r1 = create_test_result(52.0, 13.0, 12, 180.0, 30.0);
        let r2 = create_test_result(52.0, 13.0, 13, 195.0, 35.0);

        let variance = VarianceFlags::detect(&r1, &r2);

        assert!(!variance.latitude);
        assert!(!variance.longitude);
        assert!(variance.datetime);
    }

    #[test]
    fn test_variance_detection_coordinate_sweep() {
        let r1 = create_test_result(52.0, 13.0, 12, 180.0, 30.0);
        let r2 = create_test_result(52.1, 13.0, 12, 180.5, 30.5);

        let variance = VarianceFlags::detect(&r1, &r2);

        assert!(variance.latitude);
        assert!(!variance.longitude);
        assert!(!variance.datetime);
    }

    #[test]
    fn test_table_formatter_column_headers_time_series() {
        let formatter = TableFormatter {
            variance: VarianceFlags {
                datetime: true,
                ..Default::default()
            },
            elevation_angle: false,
            apply_refraction: false,
        };

        let headers = formatter.column_headers();
        assert_eq!(headers, vec!["DateTime", "Azimuth", "Zenith"]);
    }

    #[test]
    fn test_table_formatter_column_headers_coord_sweep() {
        let formatter = TableFormatter {
            variance: VarianceFlags {
                latitude: true,
                longitude: true,
                ..Default::default()
            },
            elevation_angle: true,
            apply_refraction: false,
        };

        let headers = formatter.column_headers();
        assert_eq!(
            headers,
            vec!["Latitude", "Longitude", "Azimuth", "Elevation"]
        );
    }

    #[test]
    fn test_header_section_formatting() {
        let result = create_test_result(52.0, 13.0, 12, 180.0, 30.0);
        let variance = VarianceFlags {
            datetime: true,
            ..Default::default()
        };

        let mut output = Vec::new();
        write_header_section(&mut output, &result, &variance).unwrap();

        let text = String::from_utf8(output).unwrap();
        assert!(text.contains("Latitude:    52.000000°"));
        assert!(text.contains("Longitude:   13.000000°"));
        assert!(!text.contains("DateTime:"));
    }
}
