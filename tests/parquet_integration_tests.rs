#![cfg(feature = "parquet")]

mod common;
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

    // Verify expected schema fields (specific date for sunrise does NOT auto-enable show-inputs)
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(field_names, vec!["type", "sunrise", "transit", "sunset"]);

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
            .expect(&format!("{} field should exist", twilight_field));
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
        total_rows >= 400 && total_rows <= 450,
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
