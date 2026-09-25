#!/usr/bin/env python3
"""Regenerate JPL reference fixtures manually; requires only Python's standard library."""

import calendar
import csv
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
from pathlib import Path
import random
import urllib.request

ENDPOINT = "https://ssd.jpl.nasa.gov/api/horizons_file.api"
SEED = 20240925
SAMPLE_COUNT = 3770
OUTPUT = Path(__file__).resolve().parent


def samples():
    rng = random.Random(SEED)
    edges = {(-80, 0), (80, 0), (0, 0), (0, -180), (0, 180), (-80, 180), (80, -180)}
    grid = [(lat, lon) for lat in range(-80, 81, 20) for lon in range(-180, 181, 30)]
    sites = sorted(edges | set(rng.sample([site for site in grid if site not in edges], 16)))
    seasons = [(3, 20), (6, 21), (9, 22), (12, 21)]
    extra_rng = random.Random(SEED + 1)
    time_grid = [datetime(2024, 1, 1) + timedelta(hours=3 * i) for i in range(366 * 8)]
    per_site, remainder = divmod(SAMPLE_COUNT, len(sites))
    for index, (lat, lon) in enumerate(sites):
        times = {datetime(2024, month, rng.randint(1, calendar.monthrange(2024, month)[1]),
                          rng.randrange(8) * 3) for month in range(1, 13)}
        times.update(datetime(2024, month, day, hour)
                     for (month, day), hour in zip(seasons, [0, 6, 12, 18]))
        if (lat, lon) == (0, 0):
            times.update(datetime(2024, month, day, hour)
                         for month, day in seasons for hour in [6, 12, 18])
        # Preserve the original monthly/edge samples and fill evenly by site.
        count = per_site + (index < remainder)
        times.update(extra_rng.sample([dt for dt in time_grid if dt not in times], count - len(times)))
        yield lat, lon, sorted(times)


def main():
    rows, sources = [], []
    for lat, lon, times in samples():
        params = {
            "COMMAND": "'10'", "EPHEM_TYPE": "'OBSERVER'",
            "CENTER": "'coord@399'", "COORD_TYPE": "'GEODETIC'",
            "SITE_COORD": f"'{lon},{lat},0'",
            "TLIST": "\n".join(f"'{dt:%Y-%m-%d %H:%M:%S}'" for dt in times),
            "TLIST_TYPE": "'CAL'", "TIME_TYPE": "'UT'", "QUANTITIES": "'4'",
            "APPARENT": "'AIRLESS'", "CSV_FORMAT": "'YES'", "TIME_DIGITS": "'SECONDS'",
            "EXTRA_PREC": "'YES'", "OBJ_DATA": "'NO'", "ELEV_CUT": "'-90'",
        }
        batch = "!$$SOF\n" + "\n".join(f"{key}={value}" for key, value in params.items()) + "\n"
        boundary = "sunce-horizons-fixture"
        body = (
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"format\"\r\n\r\njson\r\n"
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"input\"; filename=\"input.txt\"\r\n"
            f"Content-Type: text/plain\r\n\r\n{batch}\r\n--{boundary}--\r\n"
        ).encode()
        request = urllib.request.Request(ENDPOINT, data=body,
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
        # Requests are sequential, as required by the JPL API fair-use policy.
        with urllib.request.urlopen(request, timeout=60) as response:
            raw = response.read()
        payload = json.loads(raw)
        if "error" in payload:
            raise RuntimeError(payload["error"])
        if payload["signature"]["version"] != "1.0":
            raise RuntimeError("Horizons file API version changed; review the response format")
        result = payload["result"]
        if "NO (AIRLESS)" not in result or "{source: DE441}" not in result:
            raise RuntimeError("Reference model/settings changed; review before updating fixtures")
        table = result.split("$$SOE\n", 1)[1].split("$$EOE", 1)[0]
        returned = []
        for row in csv.reader(io.StringIO(table), skipinitialspace=True):
            dt = datetime.strptime(row[0].strip(), "%Y-%b-%d %H:%M:%S.%f")
            returned.append(dt)
            azimuth, elevation = row[3].strip(), row[4].strip()
            assert 0 <= float(azimuth) <= 360 and -90 <= float(elevation) <= 90
            rows.append([lat, lon, dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"), azimuth, elevation])
        if returned != times:
            raise RuntimeError("Horizons returned unexpected timestamps")
        sources.append({"latitude": lat, "longitude": lon, "request": {key: value for key, value in params.items() if key != "TLIST"},
                        "signature": payload["signature"], "response_sha256": hashlib.sha256(raw).hexdigest(),
                        "header": "\n".join(line.rstrip() for line in result.split("$$SOE", 1)[0].splitlines()
                            if line.startswith(("Target body name", "Center body name", "EOP file", "EOP coverage")))})
        print(f"{lat:3},{lon:4}: {len(returned)} reference positions", flush=True)
    # Only replace fixtures once all requests have succeeded.
    with (OUTPUT / "positions.csv").open("w", newline="") as output:
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(["latitude", "longitude", "dateTime", "azimuth", "elevation"])
        writer.writerows(rows)
    metadata = {"endpoint": ENDPOINT, "retrieved_utc": datetime.now(timezone.utc).isoformat(),
                "seed": SEED, "sample_count": SAMPLE_COUNT, "sources": sources}
    (OUTPUT / "sources.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(f"Saved {len(rows)} positions from {len(sources)} sites")


if __name__ == "__main__":
    main()
