//! Parquet output format support.

use crate::compute::CalculationResult;
use crate::data::{Command, Parameters};
use crate::output::{normalize_position_result, normalize_sunrise_result, position_angle_label};
use arrow::array::{ArrayRef, Float64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::io::Write;
use std::sync::Arc;

const BATCH_SIZE: usize = 8192;

// Helper to create optional Float64Builder
fn opt_f64_builder(condition: bool) -> Option<Float64Builder> {
    condition.then(|| Float64Builder::with_capacity(BATCH_SIZE))
}

// Helper to create optional StringBuilder
fn opt_string_builder(condition: bool, capacity: usize) -> Option<StringBuilder> {
    condition.then(|| StringBuilder::with_capacity(BATCH_SIZE, capacity))
}

// Helper to finish and reset Float64Builder
fn finish_and_reset_f64(builder: &mut Option<Float64Builder>, arrays: &mut Vec<ArrayRef>) {
    if let Some(b) = builder {
        arrays.push(Arc::new(b.finish()) as ArrayRef);
        *b = Float64Builder::with_capacity(BATCH_SIZE);
    }
}

// Helper to finish and reset StringBuilder
fn finish_and_reset_string(
    builder: &mut Option<StringBuilder>,
    arrays: &mut Vec<ArrayRef>,
    capacity: usize,
) {
    if let Some(b) = builder {
        arrays.push(Arc::new(b.finish()) as ArrayRef);
        *b = StringBuilder::with_capacity(BATCH_SIZE, capacity);
    }
}

fn finish_and_reset_f64_required(builder: &mut Float64Builder, arrays: &mut Vec<ArrayRef>) {
    arrays.push(Arc::new(builder.finish()) as ArrayRef);
    *builder = Float64Builder::with_capacity(BATCH_SIZE);
}

fn finish_and_reset_string_required(
    builder: &mut StringBuilder,
    arrays: &mut Vec<ArrayRef>,
    capacity: usize,
) {
    arrays.push(Arc::new(builder.finish()) as ArrayRef);
    *builder = StringBuilder::with_capacity(BATCH_SIZE, capacity);
}

pub fn write_parquet<W: Write + Send>(
    results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
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
    results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
    params: &Parameters,
    writer: W,
) -> std::io::Result<usize> {
    let show_inputs = params.output.should_show_inputs();
    let elevation_angle = params.output.elevation_angle;

    let schema = build_position_schema(show_inputs, elevation_angle, params.environment.refraction);
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut parquet_writer = ArrowWriter::try_new(writer, schema.clone(), Some(props))
        .map_err(|e| std::io::Error::other(format!("Parquet writer error: {}", e)))?;

    let mut lat_builder = opt_f64_builder(show_inputs);
    let mut lon_builder = opt_f64_builder(show_inputs);
    let mut elev_builder = opt_f64_builder(show_inputs);
    let mut press_builder = opt_f64_builder(show_inputs && params.environment.refraction);
    let mut temp_builder = opt_f64_builder(show_inputs && params.environment.refraction);
    let mut dt_builder = StringBuilder::with_capacity(BATCH_SIZE, 30);
    let mut deltat_builder = opt_f64_builder(show_inputs);
    let mut az_builder = Float64Builder::with_capacity(BATCH_SIZE);
    let mut angle_builder = Float64Builder::with_capacity(BATCH_SIZE);

    let mut batch_count = 0;
    let mut total_count = 0;

    for result_or_err in results {
        let result = result_or_err.map_err(std::io::Error::other)?;
        let row = normalize_position_result(&result)
            .ok_or_else(|| std::io::Error::other("Unexpected calculation result for position"))?;

        if show_inputs {
            lat_builder.as_mut().unwrap().append_value(row.lat);
            lon_builder.as_mut().unwrap().append_value(row.lon);
            elev_builder
                .as_mut()
                .unwrap()
                .append_value(params.environment.elevation);
            if params.environment.refraction {
                press_builder
                    .as_mut()
                    .unwrap()
                    .append_value(params.environment.pressure);
                temp_builder
                    .as_mut()
                    .unwrap()
                    .append_value(params.environment.temperature);
            }
            deltat_builder.as_mut().unwrap().append_value(row.deltat);
        }

        dt_builder.append_value(row.datetime.to_rfc3339());
        az_builder.append_value(row.azimuth);
        angle_builder.append_value(row.angle(elevation_angle));

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
    dt_builder: &mut StringBuilder,
    deltat_builder: &mut Option<Float64Builder>,
    az_builder: &mut Float64Builder,
    angle_builder: &mut Float64Builder,
) -> std::io::Result<()> {
    let mut arrays: Vec<ArrayRef> = Vec::new();

    finish_and_reset_f64(lat_builder, &mut arrays);
    finish_and_reset_f64(lon_builder, &mut arrays);
    finish_and_reset_f64(elev_builder, &mut arrays);
    finish_and_reset_f64(press_builder, &mut arrays);
    finish_and_reset_f64(temp_builder, &mut arrays);

    finish_and_reset_string_required(dt_builder, &mut arrays, 30);
    finish_and_reset_f64(deltat_builder, &mut arrays);

    finish_and_reset_f64_required(az_builder, &mut arrays);
    finish_and_reset_f64_required(angle_builder, &mut arrays);

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

    fields.push(Field::new("dateTime", DataType::Utf8, false));

    if show_inputs {
        fields.push(Field::new("deltaT", DataType::Float64, false));
    }

    fields.push(Field::new("azimuth", DataType::Float64, false));

    let angle_name = position_angle_label(elevation_angle);
    fields.push(Field::new(angle_name, DataType::Float64, false));

    Arc::new(Schema::new(fields))
}

fn write_sunrise_parquet<W: Write + Send>(
    results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
    params: &Parameters,
    writer: W,
) -> std::io::Result<usize> {
    let show_inputs = params.output.should_show_inputs();
    let show_twilight = params.calculation.twilight;

    let schema = build_sunrise_schema(show_inputs, show_twilight);
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut parquet_writer = ArrowWriter::try_new(writer, schema.clone(), Some(props))
        .map_err(|e| std::io::Error::other(format!("Parquet writer error: {}", e)))?;

    let mut lat_builder = opt_f64_builder(show_inputs);
    let mut lon_builder = opt_f64_builder(show_inputs);
    let mut date_builder = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 10);
    let mut deltat_builder = opt_f64_builder(show_inputs);
    let mut type_builder = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 10);
    let mut sunrise_builder = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25);
    let mut transit_builder = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25);
    let mut sunset_builder = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25);

    let mut civil_start_builder = opt_string_builder(show_twilight, BATCH_SIZE * 25);
    let mut civil_end_builder = opt_string_builder(show_twilight, BATCH_SIZE * 25);
    let mut nautical_start_builder = opt_string_builder(show_twilight, BATCH_SIZE * 25);
    let mut nautical_end_builder = opt_string_builder(show_twilight, BATCH_SIZE * 25);
    let mut astro_start_builder = opt_string_builder(show_twilight, BATCH_SIZE * 25);
    let mut astro_end_builder = opt_string_builder(show_twilight, BATCH_SIZE * 25);

    let mut batch_count = 0;
    let mut total_count = 0;

    let append_opt_time =
        |builder: &mut Option<StringBuilder>,
         time: Option<&chrono::DateTime<chrono::FixedOffset>>| {
            if let Some(b) = builder {
                match time {
                    Some(t) => append_time(b, t),
                    None => b.append_null(),
                }
            }
        };

    for result_or_err in results {
        let result = result_or_err.map_err(std::io::Error::other)?;
        let row = normalize_sunrise_result(&result)
            .ok_or_else(|| std::io::Error::other("Unexpected calculation result for sunrise"))?;

        if show_inputs {
            lat_builder.as_mut().unwrap().append_value(row.lat);
            lon_builder.as_mut().unwrap().append_value(row.lon);
        }

        date_builder.append_value(row.date_time.to_rfc3339());

        if show_inputs {
            deltat_builder.as_mut().unwrap().append_value(row.deltat);
        }

        type_builder.append_value(row.type_label);

        match row.type_label {
            "NORMAL" => {
                append_time(
                    &mut sunrise_builder,
                    row.sunrise.as_ref().expect("sunrise expected"),
                );
                append_time(&mut transit_builder, &row.transit);
                append_time(
                    &mut sunset_builder,
                    row.sunset.as_ref().expect("sunset expected"),
                );
            }
            "ALL_DAY" | "ALL_NIGHT" => {
                sunrise_builder.append_null();
                append_time(&mut transit_builder, &row.transit);
                sunset_builder.append_null();
            }
            _ => return Err(std::io::Error::other("Unknown sunrise type")),
        }

        if show_twilight {
            append_opt_time(&mut civil_start_builder, row.civil_start.as_ref());
            append_opt_time(&mut civil_end_builder, row.civil_end.as_ref());
            append_opt_time(&mut nautical_start_builder, row.nautical_start.as_ref());
            append_opt_time(&mut nautical_end_builder, row.nautical_end.as_ref());
            append_opt_time(&mut astro_start_builder, row.astro_start.as_ref());
            append_opt_time(&mut astro_end_builder, row.astro_end.as_ref());
        }

        batch_count += 1;
        total_count += 1;

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

    finish_and_reset_f64(lat_builder, &mut arrays);
    finish_and_reset_f64(lon_builder, &mut arrays);

    finish_and_reset_string_required(date_builder, &mut arrays, BATCH_SIZE * 10);
    finish_and_reset_f64(deltat_builder, &mut arrays);

    finish_and_reset_string_required(type_builder, &mut arrays, BATCH_SIZE * 10);
    finish_and_reset_string_required(sunrise_builder, &mut arrays, BATCH_SIZE * 25);
    finish_and_reset_string_required(transit_builder, &mut arrays, BATCH_SIZE * 25);
    finish_and_reset_string_required(sunset_builder, &mut arrays, BATCH_SIZE * 25);

    finish_and_reset_string(civil_start_builder, &mut arrays, BATCH_SIZE * 25);
    finish_and_reset_string(civil_end_builder, &mut arrays, BATCH_SIZE * 25);
    finish_and_reset_string(nautical_start_builder, &mut arrays, BATCH_SIZE * 25);
    finish_and_reset_string(nautical_end_builder, &mut arrays, BATCH_SIZE * 25);
    finish_and_reset_string(astro_start_builder, &mut arrays, BATCH_SIZE * 25);
    finish_and_reset_string(astro_end_builder, &mut arrays, BATCH_SIZE * 25);

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
