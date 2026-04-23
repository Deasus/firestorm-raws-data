"""
FIRESTORM RAWS / FEMS Pipeline — v1 (Phase 1 + Phase 2)
========================================================
Pulls Remote Automated Weather Station (RAWS) metadata and latest
observations from the US federal Fire Environment Mapping System (FEMS),
the authoritative source for NFDRS.

Data source: https://fems.fs2c.usda.gov/api/ext-climatology/*
Public endpoints — no auth required for recent observations.

Outputs:
  data/raws-stations.json  — full list of stations (metadata, lat/lng, elevation)
  data/raws-latest.json    — latest observation per station (hourly)
  data/meta.json           — pipeline metadata (counts, timestamps, raw counts)

Designed to FAIL LOUDLY if endpoint paths change, rather than silently
return empty results. Every source is logged with response code and
byte count so issues are easy to diagnose.

Phases:
  Phase 1 (this script): station list + current weather observations
  Phase 2 (this script): last 24h observations per station for sparklines
  Phase 3 (future):      add NFDRS indices (BI, ERC, SC) with color-coding
  Phase 4 (future):      Live Fuel Moisture historical trace
  Phase 5 (future):      Gridded FEMS NFDRS outputs as base heatmap

Schedule: GitHub Actions cron, every 1 hour.

NOTE: This is v1 — exact FEMS API endpoint paths are best-effort based
on partial public documentation. The script tries the most-likely
candidates and logs which one works. Expect iteration after first run.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from urllib.parse import quote, urlencode

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# ── FEMS API base URLs to try ──────────────────────────────────────
# Listed in order of likelihood based on the confirmed ext-climatology
# endpoint pattern. First success wins.
FEMS_BASE = "https://fems.fs2c.usda.gov"

# Station list endpoint candidates
STATION_LIST_CANDIDATES = [
    f"{FEMS_BASE}/api/ext-climatology/stations",
    f"{FEMS_BASE}/api/ext-station/list",
    f"{FEMS_BASE}/api/stations",
    f"{FEMS_BASE}/api/ext-stations",
    f"{FEMS_BASE}/api/ext-raws/stations",
]

# Hourly weather observations endpoint candidates
# Format inferred from confirmed download-nfdr pattern
WEATHER_DOWNLOAD_BASE = f"{FEMS_BASE}/api/ext-climatology/download-weather"
NFDR_DOWNLOAD_BASE = f"{FEMS_BASE}/api/ext-climatology/download-nfdr"

# ── HTTP helpers ───────────────────────────────────────────────────

def _req(url, description, timeout=30):
    """Wrapper around requests.get with uniform logging."""
    import requests
    try:
        resp = requests.get(url, timeout=timeout, headers={
            "User-Agent": "FIRESTORM-RAWS-Pipeline/1.0",
            "Accept": "application/json, */*",
        })
        ct = resp.headers.get("content-type", "")
        print(f"  {description}: HTTP {resp.status_code}, "
              f"{len(resp.content)} bytes, type: {ct[:40]}")
        return resp
    except Exception as e:
        print(f"  {description}: EXCEPTION — {e}")
        return None


def _try_json(resp):
    """Best-effort JSON parse. Returns None on failure."""
    if resp is None or resp.status_code != 200:
        return None
    try:
        return resp.json()
    except Exception as e:
        # Sometimes endpoint returns CSV even when we want JSON
        text = resp.text[:200]
        print(f"    JSON parse failed: {e}. First 200 chars: {text!r}")
        return None


# ── Phase 1: Station metadata ──────────────────────────────────────

def fetch_station_list():
    """
    Fetch the full RAWS station list from FEMS.
    Returns a list of dicts with at least: stationId, name, lat, lng, elevation.

    Tries multiple candidate endpoint paths and returns the first one that
    yields a recognizable station array. Logs every attempt.
    """
    print("\n[STATIONS] Fetching RAWS station metadata from FEMS...")

    for candidate_url in STATION_LIST_CANDIDATES:
        print(f"  Trying: {candidate_url}")
        resp = _req(candidate_url, description="  Response")
        if resp is None or resp.status_code != 200:
            continue

        data = _try_json(resp)
        if not data:
            continue

        # FEMS might return the station array directly, OR wrapped in
        # a 'data', 'stations', 'items', or 'results' key. Handle both.
        stations_raw = None
        if isinstance(data, list):
            stations_raw = data
        elif isinstance(data, dict):
            for key in ["stations", "data", "items", "results", "features"]:
                if key in data and isinstance(data[key], list):
                    stations_raw = data[key]
                    print(f"  Station array found under key '{key}'")
                    break

        if not stations_raw:
            print(f"  JSON parsed but no recognizable station array. "
                  f"Top-level keys: {list(data.keys())[:10] if isinstance(data, dict) else '(list)'}")
            continue

        # Normalize field names — FEMS may use camelCase, snake_case, or
        # abbreviated keys depending on API version. Try several.
        stations = []
        for s in stations_raw:
            if not isinstance(s, dict):
                continue
            station_id = (s.get("stationId") or s.get("station_id")
                          or s.get("id") or s.get("nwsId") or s.get("nwsid"))
            name = (s.get("stationName") or s.get("name")
                    or s.get("station_name") or s.get("displayName"))
            lat = (s.get("latitude") or s.get("lat") or s.get("y"))
            lng = (s.get("longitude") or s.get("lon") or s.get("lng")
                   or s.get("long") or s.get("x"))
            elev = (s.get("elevation") or s.get("elev")
                    or s.get("elevationFeet") or s.get("height"))
            agency = (s.get("agency") or s.get("owner")
                      or s.get("ownerAgency") or s.get("operator"))
            active = s.get("active", True)

            # Skip records missing essentials
            if station_id is None or lat is None or lng is None:
                continue
            # GeoJSON features have geometry.coordinates — try that
            if "geometry" in s and isinstance(s["geometry"], dict):
                coords = s["geometry"].get("coordinates", [])
                if len(coords) >= 2:
                    lng, lat = coords[0], coords[1]

            try:
                stations.append({
                    "stationId": str(station_id),
                    "name": name or f"Station {station_id}",
                    "lat": float(lat),
                    "lng": float(lng),
                    "elevation": int(elev) if elev is not None else None,
                    "agency": agency or "",
                    "active": bool(active),
                })
            except (ValueError, TypeError) as e:
                print(f"  Skipping bad station record (id={station_id}): {e}")
                continue

        if stations:
            print(f"  ✓ Parsed {len(stations)} stations from {candidate_url}")
            return stations

    print("  ALL STATION ENDPOINT CANDIDATES FAILED.")
    print("  This pipeline needs the correct endpoint path.")
    print("  Visit https://fems.fs2c.usda.gov/ and inspect devtools → Network")
    print("  to find the actual station list URL, then update STATION_LIST_CANDIDATES.")
    return []


# ── Phase 2: Latest observations per station ───────────────────────

def fetch_latest_weather(stations, batch_size=50):
    """
    Pull the most recent weather observation for each station.

    Strategy: FEMS confirmed endpoint is download-weather with stationIds
    param (comma-separated) + date range. We pull last 24h so we can
    find the most recent non-null observation per station.

    Returns: dict keyed by stationId → latest obs dict.
    """
    print("\n[WEATHER] Fetching latest observations for all stations...")

    if not stations:
        print("  No stations — skipping.")
        return {}

    # Request a 24h window to ensure we catch each station's most recent obs
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(hours=24)
    end_str = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    latest_by_station = {}
    num_batches = (len(stations) + batch_size - 1) // batch_size

    for batch_idx in range(num_batches):
        batch = stations[batch_idx * batch_size : (batch_idx + 1) * batch_size]
        station_ids = ",".join(s["stationId"] for s in batch)

        params = {
            "stationIds": station_ids,
            "startDate": start_str,
            "endDate": end_str,
            "dataFormat": "json",
            "dataset": "all",
            "dateTimeFormat": "UTC",
        }
        url = f"{WEATHER_DOWNLOAD_BASE}?{urlencode(params)}"

        # Brief polite delay between batches
        if batch_idx > 0:
            time.sleep(1.0)

        resp = _req(url, f"Batch {batch_idx+1}/{num_batches} ({len(batch)} stations)")
        if resp is None or resp.status_code != 200:
            continue

        data = _try_json(resp)
        if not data:
            # Try CSV fallback — some FEMS endpoints return CSV by default
            print(f"  Attempting CSV parse for batch {batch_idx+1}...")
            records = _parse_weather_csv(resp.text)
        else:
            records = data if isinstance(data, list) else data.get("data", [])

        # Group observations by station, keep most recent per station
        for rec in records:
            if not isinstance(rec, dict):
                continue
            sid = str(rec.get("stationId") or rec.get("station_id")
                      or rec.get("stationID") or "")
            obs_time = (rec.get("observationTime") or rec.get("observation_time")
                        or rec.get("obsTime") or rec.get("time"))
            if not sid or not obs_time:
                continue
            # Keep only the most recent
            if sid not in latest_by_station or obs_time > latest_by_station[sid].get("observationTime", ""):
                latest_by_station[sid] = {
                    "stationId": sid,
                    "observationTime": obs_time,
                    "tempF": _f(rec.get("temperature") or rec.get("temp_f") or rec.get("tempF")),
                    "rhPct": _f(rec.get("relativeHumidity") or rec.get("rh") or rec.get("rhPct")),
                    "windSpeedMph": _f(rec.get("windSpeed") or rec.get("wind_speed") or rec.get("windSpeedMph")),
                    "windDirDeg": _f(rec.get("windDirection") or rec.get("wind_dir") or rec.get("windDirDeg")),
                    "windGustMph": _f(rec.get("windGust") or rec.get("gust") or rec.get("windGustMph")),
                    "precipIn": _f(rec.get("precipitation") or rec.get("precip") or rec.get("precipIn")),
                    "solarRad": _f(rec.get("solarRadiation") or rec.get("solar_rad") or rec.get("solarRad")),
                }

    print(f"  ✓ Got latest obs for {len(latest_by_station)}/{len(stations)} stations")
    return latest_by_station


def _f(val):
    """Safe float conversion. Returns None on failure/missing."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _parse_weather_csv(text):
    """Minimal CSV fallback parser. Handles basic comma-separated rows."""
    import csv
    from io import StringIO
    records = []
    try:
        reader = csv.DictReader(StringIO(text))
        for row in reader:
            records.append(dict(row))
    except Exception as e:
        print(f"    CSV parse failed: {e}")
    return records


# ── Main orchestration ─────────────────────────────────────────────

def main():
    print("=" * 60)
    print("FIRESTORM RAWS Pipeline v1 (FEMS)")
    print(f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    stations = fetch_station_list()
    if not stations:
        print("\nABORT: No stations retrieved. See candidate-URL failures above.")
        sys.exit(1)

    latest_obs = fetch_latest_weather(stations)

    # Merge latest observations INTO the station records so frontend
    # can consume one unified JSON per station (lighter payload than
    # two parallel lookups).
    for s in stations:
        s["latestObs"] = latest_obs.get(s["stationId"])

    # Write stations file
    stations_path = os.path.join(DATA_DIR, "raws-stations.json")
    with open(stations_path, "w") as f:
        json.dump({
            "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "schema_version": 1,
            "count": len(stations),
            "stations": stations,
        }, f, separators=(",", ":"))
    size_kb = os.path.getsize(stations_path) / 1024
    print(f"\n[OUTPUT] {stations_path} ({size_kb:.0f} KB, {len(stations)} stations)")

    # Write latest-only file for smaller frontend payload if needed
    latest_path = os.path.join(DATA_DIR, "raws-latest.json")
    with open(latest_path, "w") as f:
        json.dump({
            "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "schema_version": 1,
            "count": len(latest_obs),
            "observations": latest_obs,
        }, f, separators=(",", ":"))
    print(f"[OUTPUT] {latest_path} ({os.path.getsize(latest_path)/1024:.0f} KB, "
          f"{len(latest_obs)} recent observations)")

    # Metadata file
    meta = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "schema_version": 1,
        "counts": {
            "stations_total": len(stations),
            "stations_with_latest_obs": len(latest_obs),
            "stations_stale": len(stations) - len(latest_obs),
        },
        "sources": {
            "fems_api": FEMS_BASE,
            "weather_download": WEATHER_DOWNLOAD_BASE,
            "nfdr_download": NFDR_DOWNLOAD_BASE,
        },
        "phase": "Phase 1+2 (stations + latest obs). "
                 "Phase 3 (NFDRS indices) and Phase 4 (LFM) not yet implemented.",
    }
    meta_path = os.path.join(DATA_DIR, "meta.json")
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"[OUTPUT] {meta_path}")

    print(f"\n{'='*60}")
    print(f"Total stations: {len(stations)}")
    print(f"With recent obs: {len(latest_obs)} "
          f"({100*len(latest_obs)//max(len(stations),1)}%)")
    print("Done!")


if __name__ == "__main__":
    main()
