use crate::output::PositionResult;
use crate::sunrise_formatters::SunriseResultData;
use arrow::array::{ArrayRef, Float64Builder, StringBuilder, TimestampNanosecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::io::{self, Write};
use std::sync::Arc;

/// Batch size for Parquet record batches
const BATCH_SIZE: usize = 1000;

/// Helper to create optional builders based on condition
fn maybe_float64_builder(condition: bool) -> Option<Float64Builder> {
    if condition {
        Some(Float64Builder::with_capacity(BATCH_SIZE))
    } else {
        None
    }
}

/// Helper to create optional timestamp builders based on condition
fn maybe_timestamp_builder(condition: bool) -> Option<TimestampNanosecondBuilder> {
    if condition {
        Some(TimestampNanosecondBuilder::with_capacity(BATCH_SIZE))
    } else {
        None
    }
}

/// Helper to recreate optional Float64Builder with preserved capacity
fn recreate_float64_builder(builder_opt: &mut Option<Float64Builder>) {
    if let Some(builder) = builder_opt {
        let capacity = builder.capacity();
        *builder = Float64Builder::with_capacity(capacity);
    }
}

/// Helper to recreate optional TimestampNanosecondBuilder with preserved capacity
fn recreate_timestamp_builder(builder_opt: &mut Option<TimestampNanosecondBuilder>) {
    if let Some(builder) = builder_opt {
        let capacity = builder.capacity();
        *builder = TimestampNanosecondBuilder::with_capacity(capacity);
    }
}

// Field name constants to ensure consistency between schema and array building
mod field_names {
    pub const LATITUDE: &str = "latitude";
    pub const LONGITUDE: &str = "longitude";
    pub const ELEVATION: &str = "elevation";
    pub const PRESSURE: &str = "pressure";
    pub const TEMPERATURE: &str = "temperature";
    pub const DATE_TIME: &str = "dateTime";
    pub const DELTA_T: &str = "deltaT";
    pub const AZIMUTH: &str = "azimuth";
    pub const ZENITH: &str = "zenith";
    pub const ELEVATION_ANGLE: &str = "elevation-angle";
    pub const TYPE: &str = "type";
    pub const SUNRISE: &str = "sunrise";
    pub const TRANSIT: &str = "transit";
    pub const SUNSET: &str = "sunset";
    pub const CIVIL_START: &str = "civil_start";
    pub const CIVIL_END: &str = "civil_end";
    pub const NAUTICAL_START: &str = "nautical_start";
    pub const NAUTICAL_END: &str = "nautical_end";
    pub const ASTRONOMICAL_START: &str = "astronomical_start";
    pub const ASTRONOMICAL_END: &str = "astronomical_end";
}

/// Convert DateTime<FixedOffset> to nanoseconds timestamp for Arrow
fn datetime_to_nanos(dt: &chrono::DateTime<chrono::FixedOffset>) -> io::Result<i64> {
    dt.timestamp_nanos_opt()
        .ok_or_else(|| io::Error::other("DateTime timestamp out of range"))
}

/// Generic batch writer that handles common streaming logic for any batch builder
fn write_batched_parquet<B, I, W, T>(results: I, writer: W, mut batch_builder: B) -> io::Result<()>
where
    B: BatchBuilder<T>,
    I: Iterator<Item = T>,
    W: Write + Send,
{
    let schema = batch_builder.schema();

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut parquet_writer = ArrowWriter::try_new(writer, schema, Some(props))
        .map_err(|e| io::Error::other(format!("Parquet writer error: {}", e)))?;

    // Process all results
    for result in results {
        batch_builder.add_row(&result)?;

        if batch_builder.is_full() {
            let batch = batch_builder.build()?;
            parquet_writer
                .write(&batch)
                .map_err(|e| io::Error::other(format!("Parquet write error: {}", e)))?;
            batch_builder.clear();
        }
    }

    // Write final partial batch
    if !batch_builder.is_empty() {
        let batch = batch_builder.build()?;
        parquet_writer
            .write(&batch)
            .map_err(|e| io::Error::other(format!("Parquet write error: {}", e)))?;
    }

    parquet_writer
        .close()
        .map_err(|e| io::Error::other(format!("Parquet close error: {}", e)))?;

    Ok(())
}

/// Common interface for batch builders
trait BatchBuilder<T> {
    fn schema(&self) -> Arc<Schema>;
    fn add_row(&mut self, item: &T) -> io::Result<()>;
    fn is_full(&self) -> bool;
    fn is_empty(&self) -> bool;
    fn build(&mut self) -> io::Result<RecordBatch>;
    fn clear(&mut self);
}

/// Common batch state management
struct BatchState {
    capacity: usize,
    current_size: usize,
}

impl BatchState {
    fn new() -> Self {
        Self {
            capacity: BATCH_SIZE,
            current_size: 0,
        }
    }

    fn increment(&mut self) {
        self.current_size += 1;
    }

    fn is_full(&self) -> bool {
        self.current_size >= self.capacity
    }

    fn is_empty(&self) -> bool {
        self.current_size == 0
    }

    fn reset(&mut self) {
        self.current_size = 0;
    }
}

pub fn output_position_results_parquet<I, W>(
    results: I,
    writer: W,
    show_inputs: bool,
    _show_headers: bool, // Ignored for parquet - schema is self-describing
    elevation_angle: bool,
    _is_stdin: bool, // Ignored for parquet
) -> io::Result<()>
where
    I: Iterator<Item = PositionResult>,
    W: Write + Send,
{
    // Determine schema from parameters - refraction is always available when showing inputs
    let has_refraction = show_inputs;
    let batch_builder = PositionBatchBuilder::new(show_inputs, elevation_angle, has_refraction);
    write_batched_parquet(results, writer, batch_builder)
}

pub fn output_sunrise_results_parquet<I, W>(
    results: I,
    writer: W,
    show_inputs: bool,
    _show_headers: bool, // Ignored for parquet
    show_twilight: bool,
    _is_stdin: bool, // Ignored for parquet
) -> io::Result<()>
where
    I: Iterator<Item = SunriseResultData>,
    W: Write + Send,
{
    let batch_builder = SunriseBatchBuilder::new(show_inputs, show_twilight);
    write_batched_parquet(results, writer, batch_builder)
}

struct PositionBatchBuilder {
    schema: Arc<Schema>,
    state: BatchState,
    latitude_builder: Option<Float64Builder>,
    longitude_builder: Option<Float64Builder>,
    elevation_builder: Option<Float64Builder>,
    pressure_builder: Option<Float64Builder>,
    temperature_builder: Option<Float64Builder>,
    datetime_builder: TimestampNanosecondBuilder,
    delta_t_builder: Option<Float64Builder>,
    azimuth_builder: Float64Builder,
    angle_builder: Float64Builder,
    show_inputs: bool,
    elevation_angle: bool,
}

impl PositionBatchBuilder {
    fn new(show_inputs: bool, elevation_angle: bool, has_refraction: bool) -> Self {
        let schema = Self::create_schema(show_inputs, elevation_angle, has_refraction);

        Self {
            schema,
            state: BatchState::new(),
            latitude_builder: maybe_float64_builder(show_inputs),
            longitude_builder: maybe_float64_builder(show_inputs),
            elevation_builder: maybe_float64_builder(show_inputs),
            pressure_builder: maybe_float64_builder(show_inputs && has_refraction),
            temperature_builder: maybe_float64_builder(show_inputs && has_refraction),
            datetime_builder: TimestampNanosecondBuilder::with_capacity(BATCH_SIZE),
            delta_t_builder: maybe_float64_builder(show_inputs),
            azimuth_builder: Float64Builder::with_capacity(BATCH_SIZE),
            angle_builder: Float64Builder::with_capacity(BATCH_SIZE),
            show_inputs,
            elevation_angle,
        }
    }

    fn create_schema(
        show_inputs: bool,
        elevation_angle: bool,
        has_refraction: bool,
    ) -> Arc<Schema> {
        let mut fields = Vec::new();

        if show_inputs {
            fields.push(Field::new(field_names::LATITUDE, DataType::Float64, false));
            fields.push(Field::new(field_names::LONGITUDE, DataType::Float64, false));
            fields.push(Field::new(field_names::ELEVATION, DataType::Float64, false));

            if has_refraction {
                fields.push(Field::new(field_names::PRESSURE, DataType::Float64, false));
                fields.push(Field::new(
                    field_names::TEMPERATURE,
                    DataType::Float64,
                    false,
                ));
            }
        }

        fields.push(Field::new(
            field_names::DATE_TIME,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ));

        if show_inputs {
            fields.push(Field::new(field_names::DELTA_T, DataType::Float64, false));
        }

        fields.push(Field::new(field_names::AZIMUTH, DataType::Float64, false));
        fields.push(Field::new(
            if elevation_angle {
                field_names::ELEVATION_ANGLE
            } else {
                field_names::ZENITH
            },
            DataType::Float64,
            false,
        ));

        Arc::new(Schema::new(fields))
    }
}

impl BatchBuilder<PositionResult> for PositionBatchBuilder {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn add_row(&mut self, result: &PositionResult) -> io::Result<()> {
        if self.show_inputs {
            if let Some(ref mut builder) = self.latitude_builder {
                builder.append_value(result.latitude);
            }
            if let Some(ref mut builder) = self.longitude_builder {
                builder.append_value(result.longitude);
            }
            if let Some(ref mut builder) = self.elevation_builder {
                builder.append_value(result.elevation);
            }

            // Add refraction parameters if builders exist (set during schema creation)
            if let Some(ref mut builder) = self.pressure_builder {
                builder.append_value(result.pressure);
            }
            if let Some(ref mut builder) = self.temperature_builder {
                builder.append_value(result.temperature);
            }
        }

        // Convert DateTime<FixedOffset> to nanoseconds since Unix epoch
        self.datetime_builder
            .append_value(datetime_to_nanos(&result.datetime)?);

        if self.show_inputs
            && let Some(ref mut builder) = self.delta_t_builder
        {
            builder.append_value(result.delta_t);
        }

        self.azimuth_builder.append_value(result.position.azimuth());

        let angle_value = if self.elevation_angle {
            result.position.elevation_angle()
        } else {
            result.position.zenith_angle()
        };
        self.angle_builder.append_value(angle_value);

        self.state.increment();
        Ok(())
    }

    fn is_full(&self) -> bool {
        self.state.is_full()
    }

    fn is_empty(&self) -> bool {
        self.state.is_empty()
    }

    fn build(&mut self) -> io::Result<RecordBatch> {
        let mut arrays: Vec<ArrayRef> = Vec::new();

        // Build arrays in exact schema field order - this eliminates string matching
        // and ensures arrays are always in correct order matching the schema
        for field in self.schema.fields() {
            let field_name = field.name().as_str();

            if field_name == field_names::LATITUDE {
                if let Some(ref mut builder) = self.latitude_builder {
                    arrays.push(Arc::new(builder.finish()));
                }
            } else if field_name == field_names::LONGITUDE {
                if let Some(ref mut builder) = self.longitude_builder {
                    arrays.push(Arc::new(builder.finish()));
                }
            } else if field_name == field_names::ELEVATION {
                if let Some(ref mut builder) = self.elevation_builder {
                    arrays.push(Arc::new(builder.finish()));
                }
            } else if field_name == field_names::PRESSURE {
                if let Some(ref mut builder) = self.pressure_builder {
                    arrays.push(Arc::new(builder.finish()));
                }
            } else if field_name == field_names::TEMPERATURE {
                if let Some(ref mut builder) = self.temperature_builder {
                    arrays.push(Arc::new(builder.finish()));
                }
            } else if field_name == field_names::DATE_TIME {
                arrays.push(Arc::new(self.datetime_builder.finish()));
            } else if field_name == field_names::DELTA_T {
                if let Some(ref mut builder) = self.delta_t_builder {
                    arrays.push(Arc::new(builder.finish()));
                }
            } else if field_name == field_names::AZIMUTH {
                arrays.push(Arc::new(self.azimuth_builder.finish()));
            } else if field_name == field_names::ZENITH
                || field_name == field_names::ELEVATION_ANGLE
            {
                arrays.push(Arc::new(self.angle_builder.finish()));
            } else {
                return Err(io::Error::other(format!(
                    "Unexpected field in schema: {}",
                    field_name
                )));
            }
        }

        RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|e| io::Error::other(format!("RecordBatch error: {}", e)))
    }

    fn clear(&mut self) {
        // Recreate builders with preserved capacity
        recreate_float64_builder(&mut self.latitude_builder);
        recreate_float64_builder(&mut self.longitude_builder);
        recreate_float64_builder(&mut self.elevation_builder);
        recreate_float64_builder(&mut self.pressure_builder);
        recreate_float64_builder(&mut self.temperature_builder);

        let datetime_capacity = self.datetime_builder.capacity();
        self.datetime_builder = TimestampNanosecondBuilder::with_capacity(datetime_capacity);

        recreate_float64_builder(&mut self.delta_t_builder);

        let azimuth_capacity = self.azimuth_builder.capacity();
        self.azimuth_builder = Float64Builder::with_capacity(azimuth_capacity);

        let angle_capacity = self.angle_builder.capacity();
        self.angle_builder = Float64Builder::with_capacity(angle_capacity);

        self.state.reset();
    }
}

struct SunriseBatchBuilder {
    schema: Arc<Schema>,
    state: BatchState,
    latitude_builder: Option<Float64Builder>,
    longitude_builder: Option<Float64Builder>,
    datetime_builder: Option<TimestampNanosecondBuilder>,
    delta_t_builder: Option<Float64Builder>,
    type_builder: StringBuilder,
    sunrise_builder: TimestampNanosecondBuilder,
    transit_builder: TimestampNanosecondBuilder,
    sunset_builder: TimestampNanosecondBuilder,
    // Twilight builders
    civil_start_builder: Option<TimestampNanosecondBuilder>,
    civil_end_builder: Option<TimestampNanosecondBuilder>,
    nautical_start_builder: Option<TimestampNanosecondBuilder>,
    nautical_end_builder: Option<TimestampNanosecondBuilder>,
    astronomical_start_builder: Option<TimestampNanosecondBuilder>,
    astronomical_end_builder: Option<TimestampNanosecondBuilder>,
    show_inputs: bool,
    show_twilight: bool,
}

impl SunriseBatchBuilder {
    fn new(show_inputs: bool, show_twilight: bool) -> Self {
        let schema = Self::create_schema(show_inputs, show_twilight);

        Self {
            schema,
            state: BatchState::new(),
            latitude_builder: maybe_float64_builder(show_inputs),
            longitude_builder: maybe_float64_builder(show_inputs),
            datetime_builder: maybe_timestamp_builder(show_inputs),
            delta_t_builder: maybe_float64_builder(show_inputs),
            type_builder: StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 10),
            sunrise_builder: TimestampNanosecondBuilder::with_capacity(BATCH_SIZE),
            transit_builder: TimestampNanosecondBuilder::with_capacity(BATCH_SIZE),
            sunset_builder: TimestampNanosecondBuilder::with_capacity(BATCH_SIZE),
            civil_start_builder: maybe_timestamp_builder(show_twilight),
            civil_end_builder: maybe_timestamp_builder(show_twilight),
            nautical_start_builder: maybe_timestamp_builder(show_twilight),
            nautical_end_builder: maybe_timestamp_builder(show_twilight),
            astronomical_start_builder: maybe_timestamp_builder(show_twilight),
            astronomical_end_builder: maybe_timestamp_builder(show_twilight),
            show_inputs,
            show_twilight,
        }
    }

    fn create_schema(show_inputs: bool, show_twilight: bool) -> Arc<Schema> {
        let mut fields = Vec::new();

        if show_inputs {
            fields.push(Field::new(field_names::LATITUDE, DataType::Float64, false));
            fields.push(Field::new(field_names::LONGITUDE, DataType::Float64, false));
            fields.push(Field::new(
                field_names::DATE_TIME,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ));
            fields.push(Field::new(field_names::DELTA_T, DataType::Float64, false));
        }

        fields.extend([
            Field::new(field_names::TYPE, DataType::Utf8, false),
            Field::new(
                field_names::SUNRISE,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ), // Nullable for ALL_DAY/ALL_NIGHT
            Field::new(
                field_names::TRANSIT,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                field_names::SUNSET,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ), // Nullable for ALL_DAY/ALL_NIGHT
        ]);

        if show_twilight {
            fields.extend([
                Field::new(
                    field_names::CIVIL_START,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ),
                Field::new(
                    field_names::CIVIL_END,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ),
                Field::new(
                    field_names::NAUTICAL_START,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ),
                Field::new(
                    field_names::NAUTICAL_END,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ),
                Field::new(
                    field_names::ASTRONOMICAL_START,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ),
                Field::new(
                    field_names::ASTRONOMICAL_END,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ),
            ]);
        }

        Arc::new(Schema::new(fields))
    }
}

impl BatchBuilder<SunriseResultData> for SunriseBatchBuilder {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn add_row(&mut self, result: &SunriseResultData) -> io::Result<()> {
        use solar_positioning::types::SunriseResult;

        if self.show_inputs {
            if let Some(ref mut builder) = self.latitude_builder {
                builder.append_value(result.latitude);
            }
            if let Some(ref mut builder) = self.longitude_builder {
                builder.append_value(result.longitude);
            }
            if let Some(ref mut builder) = self.datetime_builder {
                builder.append_value(datetime_to_nanos(&result.datetime)?);
            }
            if let Some(ref mut builder) = self.delta_t_builder {
                builder.append_value(result.delta_t);
            }
        }

        match &result.sunrise_result {
            SunriseResult::RegularDay {
                sunrise,
                transit,
                sunset,
            } => {
                self.type_builder.append_value("NORMAL");
                self.sunrise_builder
                    .append_value(datetime_to_nanos(sunrise)?);
                self.transit_builder
                    .append_value(datetime_to_nanos(transit)?);
                self.sunset_builder.append_value(datetime_to_nanos(sunset)?);
            }
            SunriseResult::AllDay { transit } => {
                self.type_builder.append_value("ALL_DAY");
                self.sunrise_builder.append_null();
                self.transit_builder
                    .append_value(datetime_to_nanos(transit)?);
                self.sunset_builder.append_null();
            }
            SunriseResult::AllNight { transit } => {
                self.type_builder.append_value("ALL_NIGHT");
                self.sunrise_builder.append_null();
                self.transit_builder
                    .append_value(datetime_to_nanos(transit)?);
                self.sunset_builder.append_null();
            }
        }

        if self.show_twilight {
            if let Some(twilight) = &result.twilight_results {
                // Civil twilight
                match &twilight.civil {
                    SunriseResult::RegularDay {
                        sunrise, sunset, ..
                    } => {
                        if let Some(ref mut builder) = self.civil_start_builder {
                            builder.append_value(datetime_to_nanos(sunrise)?);
                        }
                        if let Some(ref mut builder) = self.civil_end_builder {
                            builder.append_value(datetime_to_nanos(sunset)?);
                        }
                    }
                    _ => {
                        if let Some(ref mut builder) = self.civil_start_builder {
                            builder.append_null();
                        }
                        if let Some(ref mut builder) = self.civil_end_builder {
                            builder.append_null();
                        }
                    }
                }

                // Nautical twilight
                match &twilight.nautical {
                    SunriseResult::RegularDay {
                        sunrise, sunset, ..
                    } => {
                        if let Some(ref mut builder) = self.nautical_start_builder {
                            builder.append_value(datetime_to_nanos(sunrise)?);
                        }
                        if let Some(ref mut builder) = self.nautical_end_builder {
                            builder.append_value(datetime_to_nanos(sunset)?);
                        }
                    }
                    _ => {
                        if let Some(ref mut builder) = self.nautical_start_builder {
                            builder.append_null();
                        }
                        if let Some(ref mut builder) = self.nautical_end_builder {
                            builder.append_null();
                        }
                    }
                }

                // Astronomical twilight
                match &twilight.astronomical {
                    SunriseResult::RegularDay {
                        sunrise, sunset, ..
                    } => {
                        if let Some(ref mut builder) = self.astronomical_start_builder {
                            builder.append_value(datetime_to_nanos(sunrise)?);
                        }
                        if let Some(ref mut builder) = self.astronomical_end_builder {
                            builder.append_value(datetime_to_nanos(sunset)?);
                        }
                    }
                    _ => {
                        if let Some(ref mut builder) = self.astronomical_start_builder {
                            builder.append_null();
                        }
                        if let Some(ref mut builder) = self.astronomical_end_builder {
                            builder.append_null();
                        }
                    }
                }
            } else {
                // No twilight data - append nulls for all twilight fields
                if let Some(ref mut builder) = self.civil_start_builder {
                    builder.append_null();
                }
                if let Some(ref mut builder) = self.civil_end_builder {
                    builder.append_null();
                }
                if let Some(ref mut builder) = self.nautical_start_builder {
                    builder.append_null();
                }
                if let Some(ref mut builder) = self.nautical_end_builder {
                    builder.append_null();
                }
                if let Some(ref mut builder) = self.astronomical_start_builder {
                    builder.append_null();
                }
                if let Some(ref mut builder) = self.astronomical_end_builder {
                    builder.append_null();
                }
            }
        }

        self.state.increment();
        Ok(())
    }

    fn is_full(&self) -> bool {
        self.state.is_full()
    }

    fn is_empty(&self) -> bool {
        self.state.is_empty()
    }

    fn build(&mut self) -> io::Result<RecordBatch> {
        let mut arrays: Vec<ArrayRef> = Vec::new();

        // Build arrays in exact schema field order - eliminates order coupling issues
        for field in self.schema.fields() {
            let field_name = field.name().as_str();

            if field_name == field_names::LATITUDE {
                if let Some(ref mut builder) = self.latitude_builder {
                    arrays.push(Arc::new(builder.finish()));
                }
            } else if field_name == field_names::LONGITUDE {
                if let Some(ref mut builder) = self.longitude_builder {
                    arrays.push(Arc::new(builder.finish()));
                }
            } else if field_name == field_names::DATE_TIME {
                if let Some(ref mut builder) = self.datetime_builder {
                    arrays.push(Arc::new(builder.finish()));
                }
            } else if field_name == field_names::DELTA_T {
                if let Some(ref mut builder) = self.delta_t_builder {
                    arrays.push(Arc::new(builder.finish()));
                }
            } else if field_name == field_names::TYPE {
                arrays.push(Arc::new(self.type_builder.finish()));
            } else if field_name == field_names::SUNRISE {
                arrays.push(Arc::new(self.sunrise_builder.finish()));
            } else if field_name == field_names::TRANSIT {
                arrays.push(Arc::new(self.transit_builder.finish()));
            } else if field_name == field_names::SUNSET {
                arrays.push(Arc::new(self.sunset_builder.finish()));
            } else if field_name == field_names::CIVIL_START {
                if let Some(ref mut builder) = self.civil_start_builder {
                    arrays.push(Arc::new(builder.finish()));
                }
            } else if field_name == field_names::CIVIL_END {
                if let Some(ref mut builder) = self.civil_end_builder {
                    arrays.push(Arc::new(builder.finish()));
                }
            } else if field_name == field_names::NAUTICAL_START {
                if let Some(ref mut builder) = self.nautical_start_builder {
                    arrays.push(Arc::new(builder.finish()));
                }
            } else if field_name == field_names::NAUTICAL_END {
                if let Some(ref mut builder) = self.nautical_end_builder {
                    arrays.push(Arc::new(builder.finish()));
                }
            } else if field_name == field_names::ASTRONOMICAL_START {
                if let Some(ref mut builder) = self.astronomical_start_builder {
                    arrays.push(Arc::new(builder.finish()));
                }
            } else if field_name == field_names::ASTRONOMICAL_END {
                if let Some(ref mut builder) = self.astronomical_end_builder {
                    arrays.push(Arc::new(builder.finish()));
                }
            } else {
                return Err(io::Error::other(format!(
                    "Unexpected field in schema: {}",
                    field_name
                )));
            }
        }

        RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|e| io::Error::other(format!("RecordBatch error: {}", e)))
    }

    fn clear(&mut self) {
        // Recreate builders with preserved capacity
        recreate_float64_builder(&mut self.latitude_builder);
        recreate_float64_builder(&mut self.longitude_builder);
        recreate_timestamp_builder(&mut self.datetime_builder);
        recreate_float64_builder(&mut self.delta_t_builder);

        // StringBuilder doesn't have a simple capacity() method, so use default capacities
        self.type_builder = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 10);

        let sunrise_capacity = self.sunrise_builder.capacity();
        self.sunrise_builder = TimestampNanosecondBuilder::with_capacity(sunrise_capacity);

        let transit_capacity = self.transit_builder.capacity();
        self.transit_builder = TimestampNanosecondBuilder::with_capacity(transit_capacity);

        let sunset_capacity = self.sunset_builder.capacity();
        self.sunset_builder = TimestampNanosecondBuilder::with_capacity(sunset_capacity);

        recreate_timestamp_builder(&mut self.civil_start_builder);
        recreate_timestamp_builder(&mut self.civil_end_builder);
        recreate_timestamp_builder(&mut self.nautical_start_builder);
        recreate_timestamp_builder(&mut self.nautical_end_builder);
        recreate_timestamp_builder(&mut self.astronomical_start_builder);
        recreate_timestamp_builder(&mut self.astronomical_end_builder);

        self.state.reset();
    }
}
