mod common;
use common::*;
use serde_json::Value;
use std::collections::HashMap;

fn output_text(args: &[&str]) -> String {
    String::from_utf8(sunce_command().args(args).output().unwrap().stdout).unwrap()
}

fn assert_core_twilight_times(record: &HashMap<String, String>) {
    assert_time_close(
        record.get("sunrise").expect("missing sunrise"),
        "2024-06-21T02:46:15+00:00",
        0,
    );
    assert_time_close(
        record.get("transit").expect("missing transit"),
        "2024-06-21T11:08:18+00:00",
        0,
    );
    assert_time_close(
        record.get("sunset").expect("missing sunset"),
        "2024-06-21T19:30:20+00:00",
        0,
    );
    assert_time_close(
        record.get("civil_start").expect("missing civil_start"),
        "2024-06-21T01:57:19+00:00",
        0,
    );
    assert_time_close(
        record.get("civil_end").expect("missing civil_end"),
        "2024-06-21T20:19:16+00:00",
        0,
    );
    assert_time_close(
        record
            .get("nautical_start")
            .expect("missing nautical_start"),
        "2024-06-21T00:38:45+00:00",
        0,
    );
    assert_time_close(
        record.get("nautical_end").expect("missing nautical_end"),
        "2024-06-21T21:37:47+00:00",
        1,
    );
}

#[test]
fn test_twilight_csv_and_json_outputs() {
    let (csv_headers, csv_rows) = parse_csv_output(&output_text(&[
        "--format=csv",
        "--timezone=UTC",
        "52.0",
        "13.4",
        "2024-06-21",
        "sunrise",
        "--twilight",
    ]));
    assert_eq!(csv_rows.len(), 1);
    let csv = csv_row_map(&csv_headers, &csv_rows[0]);
    assert_eq!(
        csv_headers,
        fields(&[
            "dateTime",
            "type",
            "sunrise",
            "transit",
            "sunset",
            "civil_start",
            "civil_end",
            "nautical_start",
            "nautical_end",
            "astronomical_start",
            "astronomical_end",
        ])
    );
    assert_eq!(
        csv.get("dateTime").map(String::as_str),
        Some("2024-06-21T00:00:00+00:00")
    );
    assert_eq!(csv.get("type").map(String::as_str), Some("NORMAL"));
    assert_core_twilight_times(&csv);
    assert_eq!(csv.get("astronomical_start").map(String::as_str), Some(""));
    assert_eq!(csv.get("astronomical_end").map(String::as_str), Some(""));

    let json = parse_json_output(&output_text(&[
        "--format=json",
        "--timezone=UTC",
        "52.0",
        "13.4",
        "2024-06-21",
        "sunrise",
        "--twilight",
    ]));
    assert_eq!(json.get("type").and_then(Value::as_str), Some("NORMAL"));
    assert!(json.get("astronomical_start").is_some_and(Value::is_null));
    assert!(json.get("astronomical_end").is_some_and(Value::is_null));
}

#[test]
fn test_without_twilight_omits_twilight_columns() {
    let no_twilight_headers = parse_csv_output(&output_text(&[
        "--format=csv",
        "--timezone=UTC",
        "52.0",
        "13.4",
        "2024-06-21",
        "sunrise",
    ]))
    .0;
    assert_eq!(
        no_twilight_headers,
        fields(&["dateTime", "type", "sunrise", "transit", "sunset"])
    );
}
