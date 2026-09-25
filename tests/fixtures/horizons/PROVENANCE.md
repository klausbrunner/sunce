# Worldwide solar-position reference sample

Source: NASA/JPL [Horizons](https://ssd.jpl.nasa.gov/horizons/manual.html),
Sun (10), Earth (399), DE441 ephemeris, observer quantity 4 (apparent azimuth
and elevation). This is independent of sunce and the solar-positioning crate.
The reference numbers are returned by JPL; sunce is never used to generate them.
`sources.json` records retrieval time, API signature, request settings, ephemeris
and Earth-orientation versions, and response hashes. Request timestamps are taken
from `positions.csv` rather than duplicated in the metadata.

## Coverage and reproduction

The test streams the complete Cartesian grid: latitude -80..80 by 20 degrees,
longitude -180..180 by 30 degrees, and all of leap year 2024 at three-hour intervals
in UTC: 342,576 positions. It checks a fixed sample against `positions.csv` and
requires every reference position to appear and the full row count to match.

`generate.py` uses seed 20240925 to select 16 locations plus seven deliberate
polar/equatorial/date-line locations. Each location has a random day and three-hour
slot in each month, plus March/June/September/December seasonal cases. The equator
at Greenwich also has 06:00, 12:00 and 18:00 cases at those dates, covering both
sides of the horizon and near-zenith positions. Both +180 and -180 are included.

The expanded sample preserves all 377 original cases and uses seed 20240926 to
fill each site's sample without replacement from the full year's three-hour slots.
It contains exactly **3,770 positions**, divided evenly across the same 23 sites
(163 or 164 per site). Requests use the file-based API to avoid URL length limits.

To explicitly refresh the references (Python standard library, network required):

```sh
python3 tests/fixtures/horizons/generate.py
cargo test --locked --test reference_accuracy_tests -- --nocapture
```

Normal tests use only the checked-in CSV and need no network or Python. Review
reference changes before committing; Horizons may revise Earth-orientation data.

## Settings and tolerance

Both calculations use geodetic coordinates, sea-level altitude, and **no atmospheric
refraction**, measuring the Sun's center. JPL azimuth runs clockwise from north;
sunce zenith is converted to elevation as `90 - zenith`. Horizons UT timestamps
in 2024 are UTC. Sunce explicitly uses SPA and delta-T 69.184 seconds (TT minus
UTC in 2024); this isolates position calculations from the delta-T estimator.

The limit is **0.005 degrees (18 arcseconds) of angular separation**, calculated
on the unit sphere. This handles 0/360-degree azimuth wraparound and the poorly
conditioned azimuth near zenith/nadir. It is an integration-test tolerance, not a
claim of SPA's intrinsic accuracy: Horizons uses observed Earth orientation and
additional apparent-position corrections; sunce does not expose UT1-UTC or polar
motion, and its CSV rounds angles to four decimals. The limit allows for these
model differences and rounding, while catching material position/time errors.
It was chosen before evaluating the full sample, not fitted to the worst result.

This fixture does not validate atmospheric refraction, sunrise/sunset, elevation
above sea level, Grena3, or years outside 2024. Those need separate reference cases.

References: [API parameters](https://ssd-api.jpl.nasa.gov/doc/horizons.html),
[file-based API](https://ssd-api.jpl.nasa.gov/doc/horizons_file.html),
[observer quantity definitions](https://ssd.jpl.nasa.gov/horizons/manual.html#observer-table).
