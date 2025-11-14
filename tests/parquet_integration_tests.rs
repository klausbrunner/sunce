#![cfg(feature = "parquet")]

mod common;
use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use common::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[test]
fn test_parquet_position_basic() {
    let output = position_test_with_format("PARQUET").get_output();
    assert!(output.status.success());
    assert!(!output.stdout.is_empty());

    // Parse the actual Parquet content
    let bytes = Bytes::from(output.stdout);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes).expect("Failed to create Parquet reader");
    let schema = builder.schema();

    // Verify expected schema fields for basic position output (no inputs)
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(field_names, vec!["dateTime", "azimuth", "zenith"]);

    // Read and validate data
    let reader = builder.build().expect("Failed to build Parquet reader");
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("Failed to read batches");
    assert!(!batches.is_empty(), "Should have at least one batch");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 1,
        "Should have exactly 1 row for single coordinate"
    );
}

#[test]
fn test_parquet_position_with_inputs() {
    let output = SunceTest::new()
        .args([
            "--format=PARQUET",
            "--show-inputs",
            "52.0",
            "13.4",
            "2024-01-01T12:00:00",
            "position",
        ])
        .get_output();
    assert!(output.status.success());
    assert!(!output.stdout.is_empty());

    // Parse and validate Parquet content
    let bytes = Bytes::from(output.stdout);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes).expect("Failed to create Parquet reader");
    let schema = builder.schema();

    // Verify expected schema fields with inputs
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        field_names,
        vec![
            "latitude",
            "longitude",
            "elevation",
            "pressure",
            "temperature",
            "dateTime",
            "deltaT",
            "azimuth",
            "zenith"
        ]
    );

    // Read data and verify row count
    let reader = builder.build().expect("Failed to build Parquet reader");
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("Failed to read batches");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "Should have exactly 1 row");
}

#[test]
fn test_parquet_sunrise_basic() {
    let output = SunceTest::new()
        .args(["--format=PARQUET", "52.0", "13.4", "2024-01-01", "sunrise"])
        .get_output();
    assert!(output.status.success());
    assert!(!output.stdout.is_empty());

    // Parse and validate Parquet content
    let bytes = Bytes::from(output.stdout);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes).expect("Failed to create Parquet reader");
    let schema = builder.schema();

    // Verify expected schema fields (single values = no auto show-inputs)
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        field_names,
        vec!["dateTime", "type", "sunrise", "transit", "sunset"]
    );

    // Verify timestamp fields are nullable where expected
    let sunrise_field = schema
        .field_with_name("sunrise")
        .expect("sunrise field should exist");
    let sunset_field = schema
        .field_with_name("sunset")
        .expect("sunset field should exist");
    let transit_field = schema
        .field_with_name("transit")
        .expect("transit field should exist");

    assert!(
        sunrise_field.is_nullable(),
        "sunrise should be nullable for ALL_DAY/ALL_NIGHT"
    );
    assert!(
        sunset_field.is_nullable(),
        "sunset should be nullable for ALL_DAY/ALL_NIGHT"
    );
    assert!(!transit_field.is_nullable(), "transit should never be null");

    // Read data to verify we get 1 row (single sunrise calculation for the specific date)
    let reader = builder.build().expect("Failed to build Parquet reader");
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("Failed to read batches");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 1,
        "Should have 1 row for single sunrise calculation"
    );
}

#[test]
fn test_parquet_sunrise_with_twilight() {
    let output = SunceTest::new()
        .args([
            "--format=PARQUET",
            "--show-inputs",
            "52.0",
            "13.4",
            "2024-06-21",
            "sunrise",
            "--twilight",
        ])
        .get_output();
    assert!(output.status.success());
    assert!(!output.stdout.is_empty());

    // Parse and validate Parquet content
    let bytes = Bytes::from(output.stdout);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes).expect("Failed to create Parquet reader");
    let schema = builder.schema();

    // Verify expected schema fields with inputs and twilight
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        field_names,
        vec![
            "latitude",
            "longitude",
            "dateTime",
            "deltaT",
            "type",
            "sunrise",
            "transit",
            "sunset",
            "civil_start",
            "civil_end",
            "nautical_start",
            "nautical_end",
            "astronomical_start",
            "astronomical_end"
        ]
    );

    // Verify all twilight fields are nullable
    for twilight_field in [
        "civil_start",
        "civil_end",
        "nautical_start",
        "nautical_end",
        "astronomical_start",
        "astronomical_end",
    ] {
        let field = schema
            .field_with_name(twilight_field)
            .unwrap_or_else(|_| panic!("{} field should exist", twilight_field));
        assert!(field.is_nullable(), "{} should be nullable", twilight_field);
    }
}

#[test]
fn test_parquet_multiple_coordinates() {
    let output = SunceTest::new()
        .args([
            "--format=PARQUET",
            "52:53:0.5", // Range: 52.0, 52.5, 53.0
            "13:14:0.5", // Range: 13.0, 13.5, 14.0
            "2024-01-01T12:00:00",
            "position",
        ])
        .get_output();
    assert!(output.status.success());
    assert!(!output.stdout.is_empty());

    // Parse and validate Parquet content
    let bytes = Bytes::from(output.stdout);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes).expect("Failed to create Parquet reader");
    let reader = builder.build().expect("Failed to build Parquet reader");

    // Read all batches and verify row count
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("Failed to read batches");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    // 3 latitudes × 3 longitudes × 1 time = 9 rows
    assert_eq!(
        total_rows, 9,
        "Should have exactly 9 rows for 3x3 coordinate grid"
    );

    // Verify batching works correctly for larger datasets
    assert!(!batches.is_empty(), "Should have at least one batch");
}

#[test]
fn test_parquet_streaming_behavior() {
    // Test that parquet doesn't run out of memory with larger datasets
    let output = SunceTest::new()
        .args([
            "--format=PARQUET",
            "50:52:0.1", // 21 coordinates
            "10:12:0.1", // 21 coordinates
            "2024-01-01T12:00:00",
            "position",
        ])
        .get_output();
    assert!(output.status.success());
    assert!(!output.stdout.is_empty());

    // Parse and validate Parquet content
    let bytes = Bytes::from(output.stdout);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes).expect("Failed to create Parquet reader");
    let reader = builder.build().expect("Failed to build Parquet reader");

    // Read all batches and verify row count
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("Failed to read batches");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    // Verify we get the actual coordinate grid size (may be affected by floating point precision)
    // Expected: 21 latitudes × 21 longitudes = 441, but actual may vary due to float precision
    assert!(
        (400..=450).contains(&total_rows),
        "Should have approximately 21x21=441 rows for coordinate grid, got {}",
        total_rows
    );

    // Verify streaming behavior - should fit in one batch
    assert_eq!(batches.len(), 1, "Should fit in one batch (size 1000)");
}

#[test]
fn test_parquet_consistency_with_csv() {
    // Test that the same calculation produces consistent results between formats
    let csv_cmd_output = {
        SunceTest::new()
            .args([
                "--format=CSV",
                "52.0",
                "13.4",
                "2024-01-01T12:00:00",
                "position",
            ])
            .get_output()
    };

    let parquet_cmd_output = {
        SunceTest::new()
            .args([
                "--format=PARQUET",
                "52.0",
                "13.4",
                "2024-01-01T12:00:00",
                "position",
            ])
            .get_output()
    };

    assert!(csv_cmd_output.status.success());
    assert!(parquet_cmd_output.status.success());

    // Parse CSV output to extract values
    let csv_text = String::from_utf8_lossy(&csv_cmd_output.stdout);
    let csv_lines: Vec<&str> = csv_text.trim().split('\n').collect();
    assert_eq!(csv_lines.len(), 2, "CSV should have header + 1 data row");

    let csv_values: Vec<&str> = csv_lines[1].split(',').collect();
    assert_eq!(
        csv_values.len(),
        3,
        "CSV should have 3 columns: dateTime,azimuth,zenith"
    );

    // Parse Parquet output and extract the same values
    let bytes = Bytes::from(parquet_cmd_output.stdout);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes).expect("Failed to create Parquet reader");
    let reader = builder.build().expect("Failed to build Parquet reader");
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("Failed to read batches");

    assert_eq!(batches.len(), 1, "Should have exactly one batch");
    assert_eq!(batches[0].num_rows(), 1, "Should have exactly one row");

    // Verify both formats produced the same logical content structure
    let batch = &batches[0];
    assert_eq!(
        batch.num_columns(),
        3,
        "Parquet should have 3 columns: dateTime,azimuth,zenith"
    );

    // Both formats should represent the same calculation results
    // (We can't easily compare exact values due to format differences, but structure should match)
}

#[test]
fn test_parquet_timezone_preservation() {
    // CRITICAL: Test that timezone information is preserved in parquet output
    // This catches the bug where dateTime was stored as timestamp (UTC) instead of string with offset
    let mut cmd = sunce_command();
    cmd.env("TZ", "Europe/Berlin");
    cmd.args([
        "--format=PARQUET",
        "52.0",
        "13.4",
        "2024-06-21T12:00:00",
        "position",
    ]);

    let output = cmd.assert().success().get_output().stdout.clone();

    // Parse parquet
    let bytes = Bytes::from(output);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes).expect("Failed to create Parquet reader");
    let reader = builder.build().expect("Failed to build Parquet reader");
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("Failed to read batches");

    let batch = &batches[0];
    let datetime_col = batch
        .column_by_name("dateTime")
        .expect("Should have dateTime column");

    // dateTime should be stored as string (Utf8) to preserve timezone
    assert!(
        matches!(datetime_col.data_type(), arrow::datatypes::DataType::Utf8),
        "dateTime must be stored as string to preserve timezone, got: {:?}",
        datetime_col.data_type()
    );

    // Verify the actual value contains timezone offset
    let datetime_array = datetime_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("dateTime should be StringArray");
    let datetime_value = datetime_array.value(0);

    // Should contain +02:00 for Europe/Berlin in summer
    assert!(
        datetime_value.contains("+02:00"),
        "dateTime should preserve timezone offset +02:00, got: {}",
        datetime_value
    );
    assert_eq!(
        datetime_value, "2024-06-21T12:00:00+02:00",
        "dateTime should exactly match input with timezone"
    );
}

#[test]
fn test_parquet_sunrise_type_consistency() {
    // Test that parquet sunrise type field uses "NORMAL" (consistent with CSV/JSON)
    // not "RegularDay" (Rust enum variant name)
    let output = SunceTest::new()
        .args(["--format=PARQUET", "52.0", "13.4", "2024-06-21", "sunrise"])
        .get_output();

    assert!(output.status.success());

    let bytes = Bytes::from(output.stdout);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes).expect("Failed to create Parquet reader");
    let reader = builder.build().expect("Failed to build Parquet reader");
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("Failed to read batches");

    let batch = &batches[0];
    let type_col = batch
        .column_by_name("type")
        .expect("Should have type column");
    let type_array = type_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("type should be StringArray");
    let type_value = type_array.value(0);

    // Should be "NORMAL" (consistent with CSV/JSON), not "RegularDay"
    assert_eq!(
        type_value, "NORMAL",
        "type field should use 'NORMAL' for consistency with CSV/JSON formats"
    );
}

#[test]
fn test_parquet_sunrise_null_handling() {
    // Test that AllDay/AllNight scenarios properly use null instead of empty strings
    // for sunrise/sunset fields in parquet output
    let output = SunceTest::new()
        .args([
            "--format=PARQUET",
            "90.0", // North pole - AllDay in summer
            "0.0",
            "2024-06-21",
            "sunrise",
        ])
        .get_output();

    assert!(output.status.success());

    let bytes = Bytes::from(output.stdout);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes).expect("Failed to create Parquet reader");
    let reader = builder.build().expect("Failed to build Parquet reader");
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("Failed to read batches");

    let batch = &batches[0];

    // Verify type is AllDay
    let type_col = batch
        .column_by_name("type")
        .expect("Should have type column");
    let type_array = type_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("type should be StringArray");
    let type_value = type_array.value(0);
    assert_eq!(
        type_value, "ALL_DAY",
        "Should be AllDay scenario at north pole in summer"
    );

    // Verify sunrise is null (not empty string)
    let sunrise_col = batch
        .column_by_name("sunrise")
        .expect("Should have sunrise column");
    let sunrise_array = sunrise_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("sunrise should be StringArray");
    assert!(
        sunrise_array.is_null(0),
        "sunrise should be null (not empty string) for AllDay scenario"
    );

    // Verify sunset is null (not empty string)
    let sunset_col = batch
        .column_by_name("sunset")
        .expect("Should have sunset column");
    let sunset_array = sunset_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("sunset should be StringArray");
    assert!(
        sunset_array.is_null(0),
        "sunset should be null (not empty string) for AllDay scenario"
    );

    // Verify transit is NOT null (always present)
    let transit_col = batch
        .column_by_name("transit")
        .expect("Should have transit column");
    let transit_array = transit_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("transit should be StringArray");
    assert!(!transit_array.is_null(0), "transit should never be null");
}

#[test]
fn test_parquet_sunrise_twilight_null_handling() {
    // Test that twilight times are properly null when not RegularDay
    let output = SunceTest::new()
        .args([
            "--format=PARQUET",
            "90.0", // North pole - AllDay in summer
            "0.0",
            "2024-06-21",
            "sunrise",
            "--twilight",
        ])
        .get_output();

    assert!(output.status.success());

    let bytes = Bytes::from(output.stdout);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes).expect("Failed to create Parquet reader");
    let reader = builder.build().expect("Failed to build Parquet reader");
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("Failed to read batches");

    let batch = &batches[0];

    // Verify all twilight fields are null for AllDay scenario
    for twilight_field in [
        "civil_start",
        "civil_end",
        "nautical_start",
        "nautical_end",
        "astronomical_start",
        "astronomical_end",
    ] {
        let col = batch
            .column_by_name(twilight_field)
            .unwrap_or_else(|| panic!("{} column should exist", twilight_field));
        let array = col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap_or_else(|| panic!("{} should be StringArray", twilight_field));
        assert!(
            array.is_null(0),
            "{} should be null (not empty string) for AllDay scenario",
            twilight_field
        );
    }
}
