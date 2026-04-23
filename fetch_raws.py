"""
FIRESTORM RAWS / FEMS Pipeline — v2 (real GraphQL schema)
==========================================================
Pulls RAWS station metadata and recent weather observations from
the US federal Fire Environment Mapping System (FEMS) via its
GraphQL API.

Endpoint: https://fems.fs2c.usda.gov/api/climatology/graphql/
Auth:     Public (no key required for station metadata + recent obs)
Schema confirmed by devtools inspection of FEMS UI, April 23 2026.

Outputs:
  data/raws-stations.json  — all stations (metadata, lat/lng, elevation, agency)
  data/raws-latest.json    — latest observation per station (hourly)
  data/meta.json           — pipeline metadata

Schedule: GitHub Actions cron, every 1 hour.

Implementation notes:
- FEMS uses GraphQL with REST-style pagination metadata in responses
- stationIds is string-typed despite station_id being integer in responses
- returnAll:true returns every public station (no auth required for public set)
- weatherObs query accepts a single stationId string; we loop across stations
  with polite delays to avoid hammering FEMS
"""

import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# ── FEMS API ─────────────────────────────────────────────────────────
FEMS_GRAPHQL = "https://fems.fs2c.usda.gov/api/climatology/graphql/"

# Polite rate: delay every N requests. FEMS is public but we should
# be respectful — this pipeline pulls ~2,000 stations hourly.
REQUEST_BATCH_SIZE = 25
INTER_BATCH_DELAY = 0.5  # seconds between batches

# ── GraphQL queries — copied verbatim from FEMS UI (devtools inspection) ──

STATION_LIST_QUERY = """
query GetAllStations(
    $hasHistoricData: TriState
    $weatherHourlyVisible: TriState
) {
    stationMetaData(
        returnAll: true
        weatherHourlyVisible: $weatherHourlyVisible
        hasHistoricData: $hasHistoricData
    ) {
        _metadata {
            page
            per_page
            total_count
            page_count
        }
        data {
            station_id
            fems_station_id
            network_name
            network_id
            wrcc_id
            state
            has_historic_data
            period_record_start
            period_record_stop
            time_zone
            time_zone_offset
            station_name
            latitude
            longitude
            aspect_direction
            elevation
            avg_annual_precip
            agency
            weather_hourly_visible
            nfdrs_hourly_visible
            produce_nfdrs
        }
    }
}
"""

WEATHER_OBS_QUERY = """
query GetWeatherObs(
    $stationId: String!
    $startDate: String
    $endDate: String
) {
    weatherObs(
        stationIds: $stationId
        startDate: $startDate
        endDate: $endDate
    ) {
        _metadata {
            page
            per_page
            total_count
            page_count
        }
        data {
            station_id
            observation_time
            observation_time_lst
            temperature
            hourly_precip
            relative_humidity
            wind_speed
            wind_direction
            peak_gust_speed
            peak_gust_dir
            sol_rad
            snow_flag
            observation_type
        }
    }
}
"""


# ── HTTP helper ──────────────────────────────────────────────────────

def gql_request(query, variables, description, timeout=60):
    """
    Fire a single GraphQL request against FEMS. Returns parsed JSON
    body or None on failure. Logs full diagnostic on every call.
    """
    import requests
    payload = {"query": query, "variables": variables}
    try:
        resp = requests.post(
            FEMS_GRAPHQL,
            json=payload,
            timeout=timeout,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "FIRESTORM-RAWS-Pipeline/2.0",
                "Origin": "https://fems.fs2c.usda.gov",
                "Referer": "https://fems.fs2c.usda.gov/ui",
            },
        )
        ct = resp.headers.get("content-type", "")
        print(f"  {description}: HTTP {resp.status_code}, "
              f"{len(resp.content)} bytes, type: {ct[:40]}")
        if resp.status_code != 200:
            print(f"    Body preview: {resp.text[:300]}")
            return None
        return resp.json()
    except Exception as e:
        print(f"  {description}: EXCEPTION — {e}")
        return None


# ── Phase 1: Station metadata ────────────────────────────────────────

def fetch_all_stations():
    """Pull the complete station list from FEMS."""
    print("\n[STATIONS] Fetching full RAWS station metadata from FEMS...")

    result = gql_request(
        STATION_LIST_QUERY,
        {
            "hasHistoricData": "ALL",
            "weatherHourlyVisible": "ALL",
        },
        description="All-stations query",
    )

    if not result or "data" not in result:
        print("  No data key in response — aborting.")
        return []

    errors = result.get("errors")
    if errors:
        print(f"  GraphQL errors: {json.dumps(errors)[:500]}")
        return []

    payload = result.get("data", {}).get("stationMetaData", {})
    metadata = payload.get("_metadata", {})
    stations_raw = payload.get("data", []) or []

    print(f"  Total reported: {metadata.get('total_count')}, "
          f"returned: {len(stations_raw)}")

    stations = []
    for s in stations_raw:
        sid = s.get("station_id")
        lat = s.get("latitude")
        lng = s.get("longitude")
        if sid is None or lat is None or lng is None:
            continue
        try:
            stations.append({
                "stationId": str(sid),
                "femsStationId": s.get("fems_station_id"),
                "wrccId": s.get("wrcc_id"),
                "name": s.get("station_name") or f"Station {sid}",
                "state": s.get("state"),
                "lat": float(lat),
                "lng": float(lng),
                "elevation": s.get("elevation"),
                "agency": s.get("agency") or "",
                "network": s.get("network_name"),
                "timeZone": s.get("time_zone"),
                "timeZoneOffset": s.get("time_zone_offset"),
                "hasHistoric": bool(s.get("has_historic_data")),
                "weatherHourly": bool(s.get("weather_hourly_visible")),
                "nfdrsHourly": bool(s.get("nfdrs_hourly_visible")),
                "produceNfdrs": bool(s.get("produce_nfdrs")),
            })
        except (ValueError, TypeError) as e:
            print(f"  Skipping malformed station record (id={sid}): {e}")
            continue

    print(f"  ✓ Parsed {len(stations)} valid stations")
    return stations


# ── Phase 2: Latest observation per station ──────────────────────────

def fetch_latest_observations(stations):
    """
    Query recent hourly observations per station, keep only the most
    recent non-forecast record.

    FEMS returns forecasts (observation_type='F') interleaved with
    observed values (observation_type='O'). For a "current conditions"
    view, prefer 'O' over 'F'. Fall back to 'F' only if a station has
    no recent observed data (typical for outages or reporting lag).
    """
    print(f"\n[WEATHER] Fetching latest obs for {len(stations)} stations...")

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(hours=6)
    end_str = end_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    active_stations = [s for s in stations if s.get("weatherHourly")]
    print(f"  Active hourly-weather stations: {len(active_stations)}")

    latest_by_station = {}
    total = len(active_stations)
    t_start = time.time()

    for i, station in enumerate(active_stations):
        sid = station["stationId"]

        if i > 0 and i % REQUEST_BATCH_SIZE == 0:
            elapsed = time.time() - t_start
            eta = (elapsed / i) * (total - i) if i else 0
            print(f"  [{i}/{total}] elapsed {elapsed:.0f}s, eta {eta:.0f}s, "
                  f"got {len(latest_by_station)} obs so far")
            time.sleep(INTER_BATCH_DELAY)

        result = gql_request(
            WEATHER_OBS_QUERY,
            {
                "stationId": sid,
                "startDate": start_str,
                "endDate": end_str,
            },
            description=f"  Station {sid}",
            timeout=30,
        )

        if not result:
            continue
        payload = result.get("data", {}).get("weatherObs")
        if not payload:
            continue
        records = payload.get("data", []) or []

        observed = [r for r in records if r.get("observation_type") == "O"]
        pool = observed if observed else records
        if not pool:
            continue
        latest = max(pool, key=lambda r: r.get("observation_time", ""))

        latest_by_station[sid] = {
            "stationId": sid,
            "observationTime": latest.get("observation_time"),
            "observationTimeLst": latest.get("observation_time_lst"),
            "tempF": latest.get("temperature"),
            "rhPct": latest.get("relative_humidity"),
            "windSpeedMph": latest.get("wind_speed"),
            "windDirDeg": latest.get("wind_direction"),
            "windGustMph": latest.get("peak_gust_speed"),
            "precipIn": latest.get("hourly_precip"),
            "solarRad": latest.get("sol_rad"),
            "snowFlag": latest.get("snow_flag"),
            "observationType": latest.get("observation_type"),
        }

    elapsed = time.time() - t_start
    print(f"  ✓ Got latest obs for {len(latest_by_station)}/{total} "
          f"stations in {elapsed:.0f}s")
    return latest_by_station


# ── Main ─────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("FIRESTORM RAWS Pipeline v2 (FEMS GraphQL)")
    print(f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    stations = fetch_all_stations()
    if not stations:
        print("\nABORT: No stations retrieved.")
        sys.exit(1)

    latest_obs = fetch_latest_observations(stations)

    # Embed latest obs into station records for single-file consumption
    for s in stations:
        s["latestObs"] = latest_obs.get(s["stationId"])

    # Write full stations file
    stations_path = os.path.join(DATA_DIR, "raws-stations.json")
    with open(stations_path, "w") as f:
        json.dump({
            "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "schema_version": 2,
            "count": len(stations),
            "stations": stations,
        }, f, separators=(",", ":"))
    size_kb = os.path.getsize(stations_path) / 1024
    print(f"\n[OUTPUT] {stations_path} ({size_kb:.0f} KB)")

    # Observations-only file (smaller, for future frequent-refresh use)
    latest_path = os.path.join(DATA_DIR, "raws-latest.json")
    with open(latest_path, "w") as f:
        json.dump({
            "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "schema_version": 2,
            "count": len(latest_obs),
            "observations": latest_obs,
        }, f, separators=(",", ":"))
    print(f"[OUTPUT] {latest_path} "
          f"({os.path.getsize(latest_path)/1024:.0f} KB)")

    meta = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "schema_version": 2,
        "counts": {
            "stations_total": len(stations),
            "stations_with_hourly": sum(1 for s in stations if s.get("weatherHourly")),
            "stations_with_latest_obs": len(latest_obs),
            "stations_with_nfdrs": sum(1 for s in stations if s.get("produceNfdrs")),
        },
        "sources": {
            "fems_graphql": FEMS_GRAPHQL,
        },
        "phase": "Phase 1+2 (stations + latest obs). "
                 "Phase 3 (NFDRS indices) not yet implemented.",
    }
    with open(os.path.join(DATA_DIR, "meta.json"), "w") as f:
        json.dump(meta, f, indent=2)
    print(f"[OUTPUT] data/meta.json")

    print(f"\n{'='*60}")
    print(f"Total stations: {len(stations)}")
    print(f"  With hourly weather: {meta['counts']['stations_with_hourly']}")
    print(f"  With recent obs:     {len(latest_obs)}")
    print(f"  With NFDRS output:   {meta['counts']['stations_with_nfdrs']}")
    print("Done!")


if __name__ == "__main__":
    main()
