//! Parquet output format support.

use crate::compute::CalculationResult;
use crate::data::{Command, Parameters};
use arrow::array::{ArrayRef, Float64Builder, StringBuilder, TimestampMillisecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::io::Write;
use std::sync::Arc;

const BATCH_SIZE: usize = 8192;

pub fn write_parquet<W: Write + Send>(
    results: Box<dyn Iterator<Item = CalculationResult>>,
    command: Command,
    params: &Parameters,
    writer: W,
) -> std::io::Result<usize> {
    match command {
        Command::Position => write_position_parquet(results, params, writer),
        Command::Sunrise => write_sunrise_parquet(results, params, writer),
    }
}

fn write_position_parquet<W: Write + Send>(
    results: Box<dyn Iterator<Item = CalculationResult>>,
    params: &Parameters,
    writer: W,
) -> std::io::Result<usize> {
    let show_inputs = params.show_inputs.unwrap_or(false);
    let elevation_angle = params.elevation_angle;

    let schema = build_position_schema(show_inputs, elevation_angle, params.refraction);
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut parquet_writer = ArrowWriter::try_new(writer, schema.clone(), Some(props))
        .map_err(|e| std::io::Error::other(format!("Parquet writer error: {}", e)))?;

    let mut lat_builder = if show_inputs {
        Some(Float64Builder::with_capacity(BATCH_SIZE))
    } else {
        None
    };
    let mut lon_builder = if show_inputs {
        Some(Float64Builder::with_capacity(BATCH_SIZE))
    } else {
        None
    };
    let mut elev_builder = if show_inputs {
        Some(Float64Builder::with_capacity(BATCH_SIZE))
    } else {
        None
    };
    let mut press_builder = if show_inputs && params.refraction {
        Some(Float64Builder::with_capacity(BATCH_SIZE))
    } else {
        None
    };
    let mut temp_builder = if show_inputs && params.refraction {
        Some(Float64Builder::with_capacity(BATCH_SIZE))
    } else {
        None
    };
    let mut dt_builder = TimestampMillisecondBuilder::with_capacity(BATCH_SIZE);
    let mut deltat_builder = if show_inputs {
        Some(Float64Builder::with_capacity(BATCH_SIZE))
    } else {
        None
    };
    let mut az_builder = Float64Builder::with_capacity(BATCH_SIZE);
    let mut angle_builder = Float64Builder::with_capacity(BATCH_SIZE);

    let mut batch_count = 0;
    let mut total_count = 0;

    for result in results {
        if let CalculationResult::Position {
            lat,
            lon,
            datetime,
            position,
            deltat,
        } = result
        {
            if show_inputs {
                lat_builder.as_mut().unwrap().append_value(lat);
                lon_builder.as_mut().unwrap().append_value(lon);
                elev_builder
                    .as_mut()
                    .unwrap()
                    .append_value(params.elevation);
                if params.refraction {
                    press_builder
                        .as_mut()
                        .unwrap()
                        .append_value(params.pressure);
                    temp_builder
                        .as_mut()
                        .unwrap()
                        .append_value(params.temperature);
                }
                deltat_builder.as_mut().unwrap().append_value(deltat);
            }

            dt_builder.append_value(datetime.timestamp_millis());
            az_builder.append_value(position.azimuth());
            let angle = if elevation_angle {
                90.0 - position.zenith_angle()
            } else {
                position.zenith_angle()
            };
            angle_builder.append_value(angle);

            batch_count += 1;
            total_count += 1;

            if batch_count >= BATCH_SIZE {
                flush_position_batch(
                    &mut parquet_writer,
                    &schema,
                    &mut lat_builder,
                    &mut lon_builder,
                    &mut elev_builder,
                    &mut press_builder,
                    &mut temp_builder,
                    &mut dt_builder,
                    &mut deltat_builder,
                    &mut az_builder,
                    &mut angle_builder,
                )?;
                batch_count = 0;
            }
        }
    }

    if batch_count > 0 {
        flush_position_batch(
            &mut parquet_writer,
            &schema,
            &mut lat_builder,
            &mut lon_builder,
            &mut elev_builder,
            &mut press_builder,
            &mut temp_builder,
            &mut dt_builder,
            &mut deltat_builder,
            &mut az_builder,
            &mut angle_builder,
        )?;
    }

    parquet_writer
        .close()
        .map_err(|e| std::io::Error::other(format!("Failed to close parquet: {}", e)))?;

    Ok(total_count)
}

#[allow(clippy::too_many_arguments)]
fn flush_position_batch<W: Write + Send>(
    writer: &mut ArrowWriter<W>,
    schema: &Arc<Schema>,
    lat_builder: &mut Option<Float64Builder>,
    lon_builder: &mut Option<Float64Builder>,
    elev_builder: &mut Option<Float64Builder>,
    press_builder: &mut Option<Float64Builder>,
    temp_builder: &mut Option<Float64Builder>,
    dt_builder: &mut TimestampMillisecondBuilder,
    deltat_builder: &mut Option<Float64Builder>,
    az_builder: &mut Float64Builder,
    angle_builder: &mut Float64Builder,
) -> std::io::Result<()> {
    let mut arrays: Vec<ArrayRef> = Vec::new();

    if let Some(b) = lat_builder {
        arrays.push(Arc::new(b.finish()) as ArrayRef);
        *b = Float64Builder::with_capacity(BATCH_SIZE);
    }
    if let Some(b) = lon_builder {
        arrays.push(Arc::new(b.finish()) as ArrayRef);
        *b = Float64Builder::with_capacity(BATCH_SIZE);
    }
    if let Some(b) = elev_builder {
        arrays.push(Arc::new(b.finish()) as ArrayRef);
        *b = Float64Builder::with_capacity(BATCH_SIZE);
    }
    if let Some(b) = press_builder {
        arrays.push(Arc::new(b.finish()) as ArrayRef);
        *b = Float64Builder::with_capacity(BATCH_SIZE);
    }
    if let Some(b) = temp_builder {
        arrays.push(Arc::new(b.finish()) as ArrayRef);
        *b = Float64Builder::with_capacity(BATCH_SIZE);
    }

    arrays.push(Arc::new(dt_builder.finish()) as ArrayRef);
    *dt_builder = TimestampMillisecondBuilder::with_capacity(BATCH_SIZE);

    if let Some(b) = deltat_builder {
        arrays.push(Arc::new(b.finish()) as ArrayRef);
        *b = Float64Builder::with_capacity(BATCH_SIZE);
    }

    arrays.push(Arc::new(az_builder.finish()) as ArrayRef);
    *az_builder = Float64Builder::with_capacity(BATCH_SIZE);

    arrays.push(Arc::new(angle_builder.finish()) as ArrayRef);
    *angle_builder = Float64Builder::with_capacity(BATCH_SIZE);

    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| std::io::Error::other(format!("Failed to create batch: {}", e)))?;

    writer
        .write(&batch)
        .map_err(|e| std::io::Error::other(format!("Failed to write batch: {}", e)))?;

    Ok(())
}

fn build_position_schema(
    show_inputs: bool,
    elevation_angle: bool,
    refraction: bool,
) -> Arc<Schema> {
    let mut fields = Vec::new();

    if show_inputs {
        fields.push(Field::new("latitude", DataType::Float64, false));
        fields.push(Field::new("longitude", DataType::Float64, false));
        fields.push(Field::new("elevation", DataType::Float64, false));
        if refraction {
            fields.push(Field::new("pressure", DataType::Float64, false));
            fields.push(Field::new("temperature", DataType::Float64, false));
        }
    }

    fields.push(Field::new(
        "dateTime",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    ));

    if show_inputs {
        fields.push(Field::new("deltaT", DataType::Float64, false));
    }

    fields.push(Field::new("azimuth", DataType::Float64, false));

    let angle_name = if elevation_angle {
        "elevation-angle"
    } else {
        "zenith"
    };
    fields.push(Field::new(angle_name, DataType::Float64, false));

    Arc::new(Schema::new(fields))
}

fn write_sunrise_parquet<W: Write + Send>(
    results: Box<dyn Iterator<Item = CalculationResult>>,
    params: &Parameters,
    writer: W,
) -> std::io::Result<usize> {
    let show_inputs = params.show_inputs.unwrap_or(false);
    let show_twilight = params.twilight;

    let schema = build_sunrise_schema(show_inputs, show_twilight);
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut parquet_writer = ArrowWriter::try_new(writer, schema.clone(), Some(props))
        .map_err(|e| std::io::Error::other(format!("Parquet writer error: {}", e)))?;

    let mut lat_builder = if show_inputs {
        Some(Float64Builder::with_capacity(BATCH_SIZE))
    } else {
        None
    };
    let mut lon_builder = if show_inputs {
        Some(Float64Builder::with_capacity(BATCH_SIZE))
    } else {
        None
    };
    let mut date_builder = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 10);
    let mut deltat_builder = if show_inputs {
        Some(Float64Builder::with_capacity(BATCH_SIZE))
    } else {
        None
    };
    let mut type_builder = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 10);
    let mut sunrise_builder = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25);
    let mut transit_builder = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25);
    let mut sunset_builder = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25);

    let mut civil_start_builder = if show_twilight {
        Some(StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25))
    } else {
        None
    };
    let mut civil_end_builder = if show_twilight {
        Some(StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25))
    } else {
        None
    };
    let mut nautical_start_builder = if show_twilight {
        Some(StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25))
    } else {
        None
    };
    let mut nautical_end_builder = if show_twilight {
        Some(StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25))
    } else {
        None
    };
    let mut astro_start_builder = if show_twilight {
        Some(StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25))
    } else {
        None
    };
    let mut astro_end_builder = if show_twilight {
        Some(StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25))
    } else {
        None
    };

    let mut batch_count = 0;
    let mut total_count = 0;

    for result in results {
        use solar_positioning::SunriseResult;

        match result {
            CalculationResult::Sunrise {
                lat,
                lon,
                date,
                result: sunrise_result,
                deltat,
            } => {
                if show_inputs {
                    lat_builder.as_mut().unwrap().append_value(lat);
                    lon_builder.as_mut().unwrap().append_value(lon);
                }

                date_builder.append_value(date.to_rfc3339());

                if show_inputs {
                    deltat_builder.as_mut().unwrap().append_value(deltat);
                }

                match &sunrise_result {
                    SunriseResult::RegularDay {
                        sunrise,
                        transit,
                        sunset,
                    } => {
                        type_builder.append_value("RegularDay");
                        append_time(&mut sunrise_builder, sunrise);
                        append_time(&mut transit_builder, transit);
                        append_time(&mut sunset_builder, sunset);
                    }
                    SunriseResult::AllDay { transit } => {
                        type_builder.append_value("AllDay");
                        sunrise_builder.append_value("");
                        append_time(&mut transit_builder, transit);
                        sunset_builder.append_value("");
                    }
                    SunriseResult::AllNight { transit } => {
                        type_builder.append_value("AllNight");
                        sunrise_builder.append_value("");
                        append_time(&mut transit_builder, transit);
                        sunset_builder.append_value("");
                    }
                }

                batch_count += 1;
                total_count += 1;
            }
            CalculationResult::SunriseWithTwilight {
                lat,
                lon,
                date,
                sunrise_sunset,
                civil,
                nautical,
                astronomical,
                deltat,
            } => {
                if show_inputs {
                    lat_builder.as_mut().unwrap().append_value(lat);
                    lon_builder.as_mut().unwrap().append_value(lon);
                }

                date_builder.append_value(date.to_rfc3339());

                if show_inputs {
                    deltat_builder.as_mut().unwrap().append_value(deltat);
                }

                match &sunrise_sunset {
                    SunriseResult::RegularDay {
                        sunrise,
                        transit,
                        sunset,
                    } => {
                        type_builder.append_value("RegularDay");
                        append_time(&mut sunrise_builder, sunrise);
                        append_time(&mut transit_builder, transit);
                        append_time(&mut sunset_builder, sunset);
                    }
                    SunriseResult::AllDay { transit } => {
                        type_builder.append_value("AllDay");
                        sunrise_builder.append_value("");
                        append_time(&mut transit_builder, transit);
                        sunset_builder.append_value("");
                    }
                    SunriseResult::AllNight { transit } => {
                        type_builder.append_value("AllNight");
                        sunrise_builder.append_value("");
                        append_time(&mut transit_builder, transit);
                        sunset_builder.append_value("");
                    }
                }

                match &civil {
                    SunriseResult::RegularDay {
                        sunrise, sunset, ..
                    } => {
                        append_time(civil_start_builder.as_mut().unwrap(), sunrise);
                        append_time(civil_end_builder.as_mut().unwrap(), sunset);
                    }
                    _ => {
                        civil_start_builder.as_mut().unwrap().append_value("");
                        civil_end_builder.as_mut().unwrap().append_value("");
                    }
                }

                match &nautical {
                    SunriseResult::RegularDay {
                        sunrise, sunset, ..
                    } => {
                        append_time(nautical_start_builder.as_mut().unwrap(), sunrise);
                        append_time(nautical_end_builder.as_mut().unwrap(), sunset);
                    }
                    _ => {
                        nautical_start_builder.as_mut().unwrap().append_value("");
                        nautical_end_builder.as_mut().unwrap().append_value("");
                    }
                }

                match &astronomical {
                    SunriseResult::RegularDay {
                        sunrise, sunset, ..
                    } => {
                        append_time(astro_start_builder.as_mut().unwrap(), sunrise);
                        append_time(astro_end_builder.as_mut().unwrap(), sunset);
                    }
                    _ => {
                        astro_start_builder.as_mut().unwrap().append_value("");
                        astro_end_builder.as_mut().unwrap().append_value("");
                    }
                }

                batch_count += 1;
                total_count += 1;
            }
            _ => continue,
        }

        if batch_count >= BATCH_SIZE {
            flush_sunrise_batch(
                &mut parquet_writer,
                &schema,
                &mut lat_builder,
                &mut lon_builder,
                &mut date_builder,
                &mut deltat_builder,
                &mut type_builder,
                &mut sunrise_builder,
                &mut transit_builder,
                &mut sunset_builder,
                &mut civil_start_builder,
                &mut civil_end_builder,
                &mut nautical_start_builder,
                &mut nautical_end_builder,
                &mut astro_start_builder,
                &mut astro_end_builder,
            )?;
            batch_count = 0;
        }
    }

    if batch_count > 0 {
        flush_sunrise_batch(
            &mut parquet_writer,
            &schema,
            &mut lat_builder,
            &mut lon_builder,
            &mut date_builder,
            &mut deltat_builder,
            &mut type_builder,
            &mut sunrise_builder,
            &mut transit_builder,
            &mut sunset_builder,
            &mut civil_start_builder,
            &mut civil_end_builder,
            &mut nautical_start_builder,
            &mut nautical_end_builder,
            &mut astro_start_builder,
            &mut astro_end_builder,
        )?;
    }

    parquet_writer
        .close()
        .map_err(|e| std::io::Error::other(format!("Failed to close parquet: {}", e)))?;

    Ok(total_count)
}

fn append_time(builder: &mut StringBuilder, time: &chrono::DateTime<chrono::FixedOffset>) {
    builder.append_value(time.format("%Y-%m-%dT%H:%M:%S%:z").to_string());
}

#[allow(clippy::too_many_arguments)]
fn flush_sunrise_batch<W: Write + Send>(
    writer: &mut ArrowWriter<W>,
    schema: &Arc<Schema>,
    lat_builder: &mut Option<Float64Builder>,
    lon_builder: &mut Option<Float64Builder>,
    date_builder: &mut StringBuilder,
    deltat_builder: &mut Option<Float64Builder>,
    type_builder: &mut StringBuilder,
    sunrise_builder: &mut StringBuilder,
    transit_builder: &mut StringBuilder,
    sunset_builder: &mut StringBuilder,
    civil_start_builder: &mut Option<StringBuilder>,
    civil_end_builder: &mut Option<StringBuilder>,
    nautical_start_builder: &mut Option<StringBuilder>,
    nautical_end_builder: &mut Option<StringBuilder>,
    astro_start_builder: &mut Option<StringBuilder>,
    astro_end_builder: &mut Option<StringBuilder>,
) -> std::io::Result<()> {
    let mut arrays: Vec<ArrayRef> = Vec::new();

    if let Some(b) = lat_builder {
        arrays.push(Arc::new(b.finish()) as ArrayRef);
        *b = Float64Builder::with_capacity(BATCH_SIZE);
    }
    if let Some(b) = lon_builder {
        arrays.push(Arc::new(b.finish()) as ArrayRef);
        *b = Float64Builder::with_capacity(BATCH_SIZE);
    }

    arrays.push(Arc::new(date_builder.finish()) as ArrayRef);
    *date_builder = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 10);

    if let Some(b) = deltat_builder {
        arrays.push(Arc::new(b.finish()) as ArrayRef);
        *b = Float64Builder::with_capacity(BATCH_SIZE);
    }

    arrays.push(Arc::new(type_builder.finish()) as ArrayRef);
    *type_builder = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 10);

    arrays.push(Arc::new(sunrise_builder.finish()) as ArrayRef);
    *sunrise_builder = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25);

    arrays.push(Arc::new(transit_builder.finish()) as ArrayRef);
    *transit_builder = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25);

    arrays.push(Arc::new(sunset_builder.finish()) as ArrayRef);
    *sunset_builder = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25);

    if let Some(b) = civil_start_builder {
        arrays.push(Arc::new(b.finish()) as ArrayRef);
        *b = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25);
    }
    if let Some(b) = civil_end_builder {
        arrays.push(Arc::new(b.finish()) as ArrayRef);
        *b = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25);
    }
    if let Some(b) = nautical_start_builder {
        arrays.push(Arc::new(b.finish()) as ArrayRef);
        *b = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25);
    }
    if let Some(b) = nautical_end_builder {
        arrays.push(Arc::new(b.finish()) as ArrayRef);
        *b = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25);
    }
    if let Some(b) = astro_start_builder {
        arrays.push(Arc::new(b.finish()) as ArrayRef);
        *b = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25);
    }
    if let Some(b) = astro_end_builder {
        arrays.push(Arc::new(b.finish()) as ArrayRef);
        *b = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25);
    }

    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| std::io::Error::other(format!("Failed to create batch: {}", e)))?;

    writer
        .write(&batch)
        .map_err(|e| std::io::Error::other(format!("Failed to write batch: {}", e)))?;

    Ok(())
}

fn build_sunrise_schema(show_inputs: bool, show_twilight: bool) -> Arc<Schema> {
    let mut fields = Vec::new();

    if show_inputs {
        fields.push(Field::new("latitude", DataType::Float64, false));
        fields.push(Field::new("longitude", DataType::Float64, false));
    }

    fields.push(Field::new("dateTime", DataType::Utf8, false));

    if show_inputs {
        fields.push(Field::new("deltaT", DataType::Float64, false));
    }

    fields.push(Field::new("type", DataType::Utf8, false));
    fields.push(Field::new("sunrise", DataType::Utf8, true));
    fields.push(Field::new("transit", DataType::Utf8, false));
    fields.push(Field::new("sunset", DataType::Utf8, true));

    if show_twilight {
        fields.push(Field::new("civil_start", DataType::Utf8, true));
        fields.push(Field::new("civil_end", DataType::Utf8, true));
        fields.push(Field::new("nautical_start", DataType::Utf8, true));
        fields.push(Field::new("nautical_end", DataType::Utf8, true));
        fields.push(Field::new("astronomical_start", DataType::Utf8, true));
        fields.push(Field::new("astronomical_end", DataType::Utf8, true));
    }

    Arc::new(Schema::new(fields))
}
