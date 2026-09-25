mod common;
use common::load_fixture_rows;

// JPL's independent ephemeris checks the complete range-expansion, calculation,
// caching, and CSV output path. See fixtures/horizons/PROVENANCE.md.
#[test]
fn worldwide_position_sample_matches_jpl_horizons() {
    use std::collections::HashMap;
    use std::process::{Command, Stdio};

    let fixtures = load_fixture_rows("horizons/positions.csv");
    let sample_count = fixtures.len();
    assert_eq!(sample_count, 3770, "reference sample size changed");
    let mut expected: HashMap<_, _> = fixtures
        .iter()
        .map(|row| {
            let key = (
                row["latitude"].parse::<i32>().unwrap(),
                row["longitude"].parse::<i32>().unwrap(),
                row["dateTime"].clone(),
            );
            let angles = (
                row["azimuth"].parse::<f64>().unwrap(),
                row["elevation"].parse::<f64>().unwrap(),
            );
            (key, angles)
        })
        .collect();
    assert_eq!(
        expected.len(),
        sample_count,
        "duplicate reference positions"
    );

    let mut child = Command::new(common::sunce_exe_path())
        .args([
            "-80:80:20",
            "-180:180:30",
            "2024",
            "position",
            "--step=3h",
            "--timezone=UTC",
            "--algorithm=spa",
            "--no-refraction",
            "--elevation=0",
            "--deltat=69.184",
            "--format=csv",
            "--show-inputs",
            "--headers",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut reader = csv::Reader::from_reader(child.stdout.take().unwrap());
    let columns = ["latitude", "longitude", "dateTime", "azimuth", "zenith"].map(|name| {
        reader
            .headers()
            .unwrap()
            .iter()
            .position(|header| header == name)
            .unwrap()
    });
    let [lat, lon, datetime, azimuth, zenith] = columns;
    let mut row_count = 0;
    let mut max_error = 0.0_f64;
    for record in reader.records() {
        let row = record.unwrap();
        row_count += 1;
        let key = (
            row[lat].parse::<f64>().unwrap() as i32,
            row[lon].parse::<f64>().unwrap() as i32,
            row[datetime].to_owned(),
        );
        if let Some((reference_azimuth, reference_elevation)) = expected.remove(&key) {
            let actual_azimuth = row[azimuth].parse::<f64>().unwrap();
            let actual_elevation = 90.0 - row[zenith].parse::<f64>().unwrap();
            // Great-circle separation handles azimuth wraparound and its
            // singularity at the zenith/nadir without loosening the tolerance.
            let delta_az = (actual_azimuth - reference_azimuth).to_radians();
            let delta_el = (actual_elevation - reference_elevation).to_radians();
            let haversine = (delta_el / 2.0).sin().powi(2)
                + actual_elevation.to_radians().cos()
                    * reference_elevation.to_radians().cos()
                    * (delta_az / 2.0).sin().powi(2);
            let error = 2.0 * haversine.clamp(0.0, 1.0).sqrt().asin().to_degrees();
            assert!(
                error <= 0.005,
                "{key:?}: separation {error}° exceeds 0.005°; actual AZ/EL={actual_azimuth}/{actual_elevation}, JPL={reference_azimuth}/{reference_elevation}"
            );
            max_error = max_error.max(error);
        }
    }
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(row_count, 9 * 13 * 366 * 8);
    assert!(
        expected.is_empty(),
        "{} reference positions missing from output",
        expected.len()
    );
    eprintln!(
        "JPL: {sample_count} samples from {row_count} rows; maximum separation {max_error:.6}°"
    );
}
