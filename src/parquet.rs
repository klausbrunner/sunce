//! Parquet output format support.

use crate::output::{DateTimeCache, EventLayout, PositionLayout, TimePrecision};
use crate::{Error, EventKind, EventRecord, PositionRecord};
use arrow_array::builder::{Float64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::io::{self, Write};
use std::sync::Arc;

const BATCH_SIZE: usize = 8192;
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
        row: &PositionRecord,
        layout: PositionLayout,
        datetime_cache: &mut DateTimeCache,
    ) {
        if layout.show_inputs {
            self.latitude
                .as_mut()
                .unwrap()
                .append_value(row.location.latitude);
            self.longitude
                .as_mut()
                .unwrap()
                .append_value(row.location.longitude);
            self.elevation
                .as_mut()
                .unwrap()
                .append_value(row.environment.elevation);
            if layout.include_refraction {
                self.pressure
                    .as_mut()
                    .unwrap()
                    .append_value(row.environment.pressure);
                self.temperature
                    .as_mut()
                    .unwrap()
                    .append_value(row.environment.temperature);
            }
            self.delta_t.as_mut().unwrap().append_value(row.deltat);
        }

        self.date_time
            .append_value(datetime_cache.format(&row.datetime));
        self.azimuth.append_value(row.position.azimuth());
        self.angle.append_value(layout.angle(row.position));
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

struct EventBatchBuilders {
    latitude: Option<Float64Builder>,
    longitude: Option<Float64Builder>,
    date: StringBuilder,
    delta_t: Option<Float64Builder>,
    day_state: StringBuilder,
    event: StringBuilder,
    time: StringBuilder,
}

impl EventBatchBuilders {
    fn new(layout: EventLayout) -> Self {
        Self {
            latitude: layout
                .show_inputs
                .then(|| Float64Builder::with_capacity(BATCH_SIZE)),
            longitude: layout
                .show_inputs
                .then(|| Float64Builder::with_capacity(BATCH_SIZE)),
            date: StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 10),
            delta_t: layout
                .show_inputs
                .then(|| Float64Builder::with_capacity(BATCH_SIZE)),
            day_state: StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 10),
            event: StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 18),
            time: StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 25),
        }
    }

    fn append_row(
        &mut self,
        row: &EventRecord,
        layout: EventLayout,
        datetime_cache: &mut DateTimeCache,
    ) {
        if layout.show_inputs {
            self.latitude.as_mut().unwrap().append_value(row.lat);
            self.longitude.as_mut().unwrap().append_value(row.lon);
            self.delta_t.as_mut().unwrap().append_value(row.deltat);
        }
        self.date.append_value(row.date.to_string());
        self.day_state.append_value(row.day_state.label());
        self.event.append_option(row.event.map(EventKind::label));
        self.time
            .append_option(row.time.as_ref().map(|time| datetime_cache.format(time)));
    }

    fn flush<W: Write + Send>(
        &mut self,
        writer: &mut ArrowWriter<W>,
        schema: &Arc<Schema>,
    ) -> io::Result<()> {
        let mut arrays = Vec::with_capacity(schema.fields().len());
        finish_optional_f64(&mut self.latitude, &mut arrays);
        finish_optional_f64(&mut self.longitude, &mut arrays);
        finish_string(&mut self.date, BATCH_SIZE * 10, &mut arrays);
        finish_optional_f64(&mut self.delta_t, &mut arrays);
        finish_string(&mut self.day_state, BATCH_SIZE * 10, &mut arrays);
        finish_string(&mut self.event, BATCH_SIZE * 18, &mut arrays);
        finish_string(&mut self.time, BATCH_SIZE * 25, &mut arrays);
        write_batch(writer, schema, arrays)
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

/// Parquet column controls.
#[derive(Debug, Default, Clone, Copy)]
pub struct Options {
    pub show_inputs: bool,
    pub elevation_angle: bool,
}

/// Export position records in bounded batches. Parquet requires a `Send` writer.
/// Input columns use each record's environment and require consistent refraction settings.
/// An empty export includes refraction columns if `show_inputs` is set.
pub fn write_positions<I, W>(records: I, options: Options, writer: W) -> Result<usize, Error>
where
    I: IntoIterator<Item = Result<PositionRecord, Error>>,
    W: Write + Send,
{
    let (layout, records) =
        PositionLayout::for_records(records, options.show_inputs, options.elevation_angle);
    write_position_rows(records, layout, writer)
}

pub(crate) fn write_position_rows<W: Write + Send>(
    records: impl IntoIterator<Item = Result<PositionRecord, Error>>,
    layout: PositionLayout,
    writer: W,
) -> Result<usize, Error> {
    let schema = build_schema(layout.csv_headers());
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(writer, schema.clone(), Some(props))
        .map_err(|e| parquet_error(format!("Parquet writer error: {e}")))?;
    let mut builders = PositionBatchBuilders::new(layout);
    let mut datetime_cache = DateTimeCache::new(TimePrecision::Fractional);
    let mut batch_count = 0;
    let mut total_count = 0;

    for row in records {
        builders.append_row(&row?, layout, &mut datetime_cache);
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

/// Export event records in bounded batches, preserving upstream errors.
pub fn write_events<I, W>(records: I, show_inputs: bool, writer: W) -> Result<usize, Error>
where
    I: IntoIterator<Item = Result<EventRecord, Error>>,
    W: Write + Send,
{
    let layout = EventLayout { show_inputs };
    let schema = build_schema(layout.csv_headers());
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(writer, schema.clone(), Some(props))
        .map_err(|e| parquet_error(format!("Parquet writer error: {e}")))?;
    let mut builders = EventBatchBuilders::new(layout);
    let mut datetime_cache = DateTimeCache::new(TimePrecision::Seconds);
    let mut batch_count = 0;
    let mut total_count = 0;

    for row in records {
        builders.append_row(&row?, layout, &mut datetime_cache);
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

fn parquet_field(name: &'static str) -> Field {
    let data_type = match name {
        "latitude" | "longitude" | "elevation" | "pressure" | "temperature" | "deltaT"
        | "azimuth" | "zenith" | "elevation-angle" => DataType::Float64,
        _ => DataType::Utf8,
    };
    let nullable = matches!(name, "event" | "time");
    Field::new(name, data_type, nullable)
}

fn build_schema(columns: Vec<&'static str>) -> Arc<Schema> {
    Arc::new(Schema::new(
        columns.into_iter().map(parquet_field).collect::<Vec<_>>(),
    ))
}
