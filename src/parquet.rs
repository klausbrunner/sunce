//! Parquet output format support.

use crate::compute::CalculationResult;
use crate::data::{Command, Parameters};
use crate::output::{
    PositionLayout, SunriseLayout, format_rfc3339, normalize_position_result,
    normalize_sunrise_result,
};
use arrow::array::{ArrayRef, Float64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::Arc;

const BATCH_SIZE: usize = 8192;
type DateTimeCache = HashMap<chrono::DateTime<chrono::FixedOffset>, String>;

fn cached_datetime(
    cache: &mut DateTimeCache,
    dt: &chrono::DateTime<chrono::FixedOffset>,
) -> String {
    cache
        .entry(*dt)
        .or_insert_with(|| format_rfc3339(dt))
        .clone()
}

fn append_time(
    builder: &mut StringBuilder,
    time: &chrono::DateTime<chrono::FixedOffset>,
    datetime_cache: &mut DateTimeCache,
) {
    builder.append_value(cached_datetime(datetime_cache, time));
}

fn parquet_error(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}

struct PositionBatchBuilders {
    latitude: Option<Float64Builder>,
    longitude: Option<Float64Builder>,
    elevation: Option<Float64Builder>,
    pressure: Option<Float64Builder>,
    temperature: Option<Float64Builder>,
    date_time: StringBuilder,
    delta_t: Option<Float64Builder>,
    azimuth: Float64Builder,
    angle: Float64Builder,
}

impl PositionBatchBuilders {
    fn new(layout: PositionLayout) -> Self {
        Self {
            latitude: layout
                .show_inputs
                .then(|| Float64Builder::with_capacity(BATCH_SIZE)),
            longitude: layout
                .show_inputs
                .then(|| Float64Builder::with_capacity(BATCH_SIZE)),
            elevation: layout
                .show_inputs
                .then(|| Float64Builder::with_capacity(BATCH_SIZE)),
            pressure: (layout.show_inputs && layout.include_refraction)
                .then(|| Float64Builder::with_capacity(BATCH_SIZE)),
            temperature: (layout.show_inputs && layout.include_refraction)
                .then(|| Float64Builder::with_capacity(BATCH_SIZE)),
            date_time: StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 30),
            delta_t: layout
                .show_inputs
                .then(|| Float64Builder::with_capacity(BATCH_SIZE)),
            azimuth: Float64Builder::with_capacity(BATCH_SIZE),
            angle: Float64Builder::with_capacity(BATCH_SIZE),
        }
    }

    fn append_row(
        &mut self,
        row: &crate::output::PositionRow,
        params: &Parameters,
        layout: PositionLayout,
        datetime_cache: &mut DateTimeCache,
    ) {
        if layout.show_inputs {
            self.latitude.as_mut().unwrap().append_value(row.lat);
            self.longitude.as_mut().unwrap().append_value(row.lon);
            self.elevation
                .as_mut()
                .unwrap()
                .append_value(params.environment.elevation);
            if layout.include_refraction {
                self.pressure
                    .as_mut()
                    .unwrap()
                    .append_value(params.environment.pressure);
                self.temperature
                    .as_mut()
                    .unwrap()
                    .append_value(params.environment.temperature);
            }
            self.delta_t.as_mut().unwrap().append_value(row.deltat);
        }

        self.date_time
            .append_value(cached_datetime(datetime_cache, &row.datetime));
        self.azimuth.append_value(row.azimuth);
        self.angle
            .append_value(row.angle(layout.uses_elevation_angle()));
    }

    fn flush<W: Write + Send>(
        &mut self,
        writer: &mut ArrowWriter<W>,
        schema: &Arc<Schema>,
    ) -> io::Result<()> {
        let mut arrays = Vec::with_capacity(schema.fields().len());
        finish_optional_f64(&mut self.latitude, &mut arrays);
        finish_optional_f64(&mut self.longitude, &mut arrays);
        finish_optional_f64(&mut self.elevation, &mut arrays);
        finish_optional_f64(&mut self.pressure, &mut arrays);
        finish_optional_f64(&mut self.temperature, &mut arrays);
        finish_string(&mut self.date_time, BATCH_SIZE * 30, &mut arrays);
        finish_optional_f64(&mut self.delta_t, &mut arrays);
        finish_f64(&mut self.azimuth, &mut arrays);
        finish_f64(&mut self.angle, &mut arrays);
        write_batch(writer, schema, arrays)
    }
}

struct SunriseBatchBuilders {
    latitude: Option<Float64Builder>,
    longitude: Option<Float64Builder>,
    date_time: StringBuilder,
    delta_t: Option<Float64Builder>,
    kind: StringBuilder,
    sunrise: StringBuilder,
    transit: StringBuilder,
    sunset: StringBuilder,
    civil_start: Option<StringBuilder>,
    civil_end: Option<StringBuilder>,
    nautical_start: Option<StringBuilder>,
    nautical_end: Option<StringBuilder>,
    astronomical_start: Option<StringBuilder>,
    astronomical_end: Option<StringBuilder>,
}

impl SunriseBatchBuilders {
    fn new(layout: SunriseLayout) -> Self {
        Self {
            latitude: layout
                .show_inputs
                .then(|| Float64Builder::with_capacity(BATCH_SIZE)),
            longitude: layout
                .show_inputs
                .then(|| Float64Builder::with_capacity(BATCH_SIZE)),
            date_time: StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 30),
            delta_t: layout
                .show_inputs
                .then(|| Float64Builder::with_capacity(BATCH_SIZE)),
            kind: StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 10),
            sunrise: StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25),
            transit: StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25),
            sunset: StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25),
            civil_start: layout
                .include_twilight
                .then(|| StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25)),
            civil_end: layout
                .include_twilight
                .then(|| StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25)),
            nautical_start: layout
                .include_twilight
                .then(|| StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25)),
            nautical_end: layout
                .include_twilight
                .then(|| StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25)),
            astronomical_start: layout
                .include_twilight
                .then(|| StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25)),
            astronomical_end: layout
                .include_twilight
                .then(|| StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25)),
        }
    }

    fn append_row(
        &mut self,
        row: &crate::output::SunriseRow,
        layout: SunriseLayout,
        datetime_cache: &mut DateTimeCache,
    ) -> io::Result<()> {
        if layout.show_inputs {
            self.latitude.as_mut().unwrap().append_value(row.lat);
            self.longitude.as_mut().unwrap().append_value(row.lon);
            self.delta_t.as_mut().unwrap().append_value(row.deltat);
        }

        self.date_time
            .append_value(cached_datetime(datetime_cache, &row.date_time));
        self.kind.append_value(row.type_label);

        match row.type_label {
            "NORMAL" => {
                append_time(
                    &mut self.sunrise,
                    row.sunrise.as_ref().expect("sunrise expected"),
                    datetime_cache,
                );
                append_time(&mut self.transit, &row.transit, datetime_cache);
                append_time(
                    &mut self.sunset,
                    row.sunset.as_ref().expect("sunset expected"),
                    datetime_cache,
                );
            }
            "ALL_DAY" | "ALL_NIGHT" => {
                self.sunrise.append_null();
                append_time(&mut self.transit, &row.transit, datetime_cache);
                self.sunset.append_null();
            }
            other => return Err(parquet_error(format!("Unknown sunrise type: {other}"))),
        }

        append_optional_time(
            &mut self.civil_start,
            row.civil_start.as_ref(),
            datetime_cache,
        );
        append_optional_time(&mut self.civil_end, row.civil_end.as_ref(), datetime_cache);
        append_optional_time(
            &mut self.nautical_start,
            row.nautical_start.as_ref(),
            datetime_cache,
        );
        append_optional_time(
            &mut self.nautical_end,
            row.nautical_end.as_ref(),
            datetime_cache,
        );
        append_optional_time(
            &mut self.astronomical_start,
            row.astro_start.as_ref(),
            datetime_cache,
        );
        append_optional_time(
            &mut self.astronomical_end,
            row.astro_end.as_ref(),
            datetime_cache,
        );

        Ok(())
    }

    fn flush<W: Write + Send>(
        &mut self,
        writer: &mut ArrowWriter<W>,
        schema: &Arc<Schema>,
    ) -> io::Result<()> {
        let mut arrays = Vec::with_capacity(schema.fields().len());
        finish_optional_f64(&mut self.latitude, &mut arrays);
        finish_optional_f64(&mut self.longitude, &mut arrays);
        finish_string(&mut self.date_time, BATCH_SIZE * 30, &mut arrays);
        finish_optional_f64(&mut self.delta_t, &mut arrays);
        finish_string(&mut self.kind, BATCH_SIZE * 10, &mut arrays);
        finish_string(&mut self.sunrise, BATCH_SIZE * 25, &mut arrays);
        finish_string(&mut self.transit, BATCH_SIZE * 25, &mut arrays);
        finish_string(&mut self.sunset, BATCH_SIZE * 25, &mut arrays);
        finish_optional_string(&mut self.civil_start, BATCH_SIZE * 25, &mut arrays);
        finish_optional_string(&mut self.civil_end, BATCH_SIZE * 25, &mut arrays);
        finish_optional_string(&mut self.nautical_start, BATCH_SIZE * 25, &mut arrays);
        finish_optional_string(&mut self.nautical_end, BATCH_SIZE * 25, &mut arrays);
        finish_optional_string(&mut self.astronomical_start, BATCH_SIZE * 25, &mut arrays);
        finish_optional_string(&mut self.astronomical_end, BATCH_SIZE * 25, &mut arrays);
        write_batch(writer, schema, arrays)
    }
}

fn append_optional_time(
    builder: &mut Option<StringBuilder>,
    time: Option<&chrono::DateTime<chrono::FixedOffset>>,
    datetime_cache: &mut DateTimeCache,
) {
    if let Some(builder) = builder {
        match time {
            Some(time) => append_time(builder, time, datetime_cache),
            None => builder.append_null(),
        }
    }
}

fn finish_optional_f64(builder: &mut Option<Float64Builder>, arrays: &mut Vec<ArrayRef>) {
    if let Some(builder) = builder {
        arrays.push(Arc::new(builder.finish()) as ArrayRef);
        *builder = Float64Builder::with_capacity(BATCH_SIZE);
    }
}

fn finish_f64(builder: &mut Float64Builder, arrays: &mut Vec<ArrayRef>) {
    arrays.push(Arc::new(builder.finish()) as ArrayRef);
    *builder = Float64Builder::with_capacity(BATCH_SIZE);
}

fn finish_string(builder: &mut StringBuilder, capacity: usize, arrays: &mut Vec<ArrayRef>) {
    arrays.push(Arc::new(builder.finish()) as ArrayRef);
    *builder = StringBuilder::with_capacity(BATCH_SIZE, capacity);
}

fn finish_optional_string(
    builder: &mut Option<StringBuilder>,
    capacity: usize,
    arrays: &mut Vec<ArrayRef>,
) {
    if let Some(builder) = builder {
        arrays.push(Arc::new(builder.finish()) as ArrayRef);
        *builder = StringBuilder::with_capacity(BATCH_SIZE, capacity);
    }
}

pub fn write_parquet<W: Write + Send>(
    results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
    command: Command,
    params: &Parameters,
    writer: W,
) -> io::Result<usize> {
    match command {
        Command::Position => write_position_parquet(results, params, writer),
        Command::Sunrise => write_sunrise_parquet(results, params, writer),
    }
}

fn write_position_parquet<W: Write + Send>(
    results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
    params: &Parameters,
    writer: W,
) -> io::Result<usize> {
    let layout = PositionLayout::from_params(params);
    let schema = build_position_schema(layout);
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(writer, schema.clone(), Some(props))
        .map_err(|e| parquet_error(format!("Parquet writer error: {e}")))?;
    let mut builders = PositionBatchBuilders::new(layout);
    let mut datetime_cache = DateTimeCache::with_capacity(2048);
    let mut batch_count = 0;
    let mut total_count = 0;

    for result in results {
        let result = result.map_err(io::Error::other)?;
        let row = normalize_position_result(&result)
            .ok_or_else(|| parquet_error("Unexpected calculation result for position"))?;
        builders.append_row(&row, params, layout, &mut datetime_cache);
        batch_count += 1;
        total_count += 1;

        if batch_count == BATCH_SIZE {
            builders.flush(&mut writer, &schema)?;
            batch_count = 0;
        }
    }

    if batch_count > 0 {
        builders.flush(&mut writer, &schema)?;
    }

    writer
        .close()
        .map_err(|e| parquet_error(format!("Failed to close parquet: {e}")))?;
    Ok(total_count)
}

fn write_sunrise_parquet<W: Write + Send>(
    results: Box<dyn Iterator<Item = Result<CalculationResult, String>>>,
    params: &Parameters,
    writer: W,
) -> io::Result<usize> {
    let layout = SunriseLayout::from_params(params);
    let schema = build_sunrise_schema(layout);
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(writer, schema.clone(), Some(props))
        .map_err(|e| parquet_error(format!("Parquet writer error: {e}")))?;
    let mut builders = SunriseBatchBuilders::new(layout);
    let mut datetime_cache = DateTimeCache::with_capacity(2048);
    let mut batch_count = 0;
    let mut total_count = 0;

    for result in results {
        let result = result.map_err(io::Error::other)?;
        let row = normalize_sunrise_result(&result)
            .ok_or_else(|| parquet_error("Unexpected calculation result for sunrise"))?;
        builders.append_row(&row, layout, &mut datetime_cache)?;
        batch_count += 1;
        total_count += 1;

        if batch_count == BATCH_SIZE {
            builders.flush(&mut writer, &schema)?;
            batch_count = 0;
        }
    }

    if batch_count > 0 {
        builders.flush(&mut writer, &schema)?;
    }

    writer
        .close()
        .map_err(|e| parquet_error(format!("Failed to close parquet: {e}")))?;
    Ok(total_count)
}

fn write_batch<W: Write + Send>(
    writer: &mut ArrowWriter<W>,
    schema: &Arc<Schema>,
    arrays: Vec<ArrayRef>,
) -> io::Result<()> {
    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| parquet_error(format!("Failed to create batch: {e}")))?;
    writer
        .write(&batch)
        .map_err(|e| parquet_error(format!("Failed to write batch: {e}")))
}

fn build_position_schema(layout: PositionLayout) -> Arc<Schema> {
    let mut fields = Vec::new();

    if layout.show_inputs {
        fields.push(Field::new("latitude", DataType::Float64, false));
        fields.push(Field::new("longitude", DataType::Float64, false));
        fields.push(Field::new("elevation", DataType::Float64, false));
        if layout.include_refraction {
            fields.push(Field::new("pressure", DataType::Float64, false));
            fields.push(Field::new("temperature", DataType::Float64, false));
        }
    }

    fields.push(Field::new("dateTime", DataType::Utf8, false));
    if layout.show_inputs {
        fields.push(Field::new("deltaT", DataType::Float64, false));
    }
    fields.push(Field::new("azimuth", DataType::Float64, false));
    fields.push(Field::new(layout.angle_label(), DataType::Float64, false));

    Arc::new(Schema::new(fields))
}

fn build_sunrise_schema(layout: SunriseLayout) -> Arc<Schema> {
    let mut fields = Vec::new();

    if layout.show_inputs {
        fields.push(Field::new("latitude", DataType::Float64, false));
        fields.push(Field::new("longitude", DataType::Float64, false));
    }

    fields.push(Field::new("dateTime", DataType::Utf8, false));
    if layout.show_inputs {
        fields.push(Field::new("deltaT", DataType::Float64, false));
    }

    fields.push(Field::new("type", DataType::Utf8, false));
    fields.push(Field::new("sunrise", DataType::Utf8, true));
    fields.push(Field::new("transit", DataType::Utf8, false));
    fields.push(Field::new("sunset", DataType::Utf8, true));

    if layout.include_twilight {
        fields.push(Field::new("civil_start", DataType::Utf8, true));
        fields.push(Field::new("civil_end", DataType::Utf8, true));
        fields.push(Field::new("nautical_start", DataType::Utf8, true));
        fields.push(Field::new("nautical_end", DataType::Utf8, true));
        fields.push(Field::new("astronomical_start", DataType::Utf8, true));
        fields.push(Field::new("astronomical_end", DataType::Utf8, true));
    }

    Arc::new(Schema::new(fields))
}
