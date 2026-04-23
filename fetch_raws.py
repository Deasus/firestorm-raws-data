"""
FIRESTORM RAWS / FEMS Pipeline — v3
==========================================================
v2 → v3 changes:
  - weatherObs query rewritten with REAL FEMS schema (discovered
    via devtools inspection 2026-04-23). v2's startDate/endDate
    arg names caused 100% HTTP 400 failures — every obs fetch was
    silently failing.
  - Field-arg names: startDateTimeRange / endDateTimeRange (was
    startDate / endDate).
  - Variable types: DateTime! (was String).
  - Added required zoomLevel: 5 (hourly) parameter.
  - Added per_page/sortBy/sortOrder to pull only the latest
    observation per station — reduces response payload ~100x.
  - Added rate-limit adaptive backoff using x-slowdown-remaining
    header so we don't exhaust FEMS's slowdown budget mid-run.

Endpoint: https://fems.fs2c.usda.gov/api/climatology/graphql/
Auth:     Public (no key)
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

# FEMS zoomLevel: 5 = hourly (confirmed via devtools).
# Other values (1/2/3/4) are daily/monthly/yearly aggregations.
FEMS_ZOOM_HOURLY = 5

# Rate-limit strategy:
#   FEMS returns x-ratelimit-limit: 1000, x-slowdown-limit: 250.
#   After ~250 reqs they start slowing us down; after 1000 they reject.
#   We use adaptive backoff: when x-slowdown-remaining drops below
#   a threshold, add a longer delay to let the window reset.
INTER_REQUEST_DELAY = 0.15        # baseline: ~6-7 req/s
SLOWDOWN_DELAY = 1.0              # when approaching slowdown limit
SLOWDOWN_THRESHOLD = 40           # switch to slower pace when this few left
BATCH_STATUS_EVERY = 100


# ── GraphQL queries — copied verbatim from FEMS UI (devtools) ───────

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

# This query shape matches FEMS UI's own GetWeatherObs request verbatim.
# The key corrections vs v2:
#   - $startDate / $endDate are DateTime! (was String)
#   - Field args are startDateTimeRange / endDateTimeRange (was startDate / endDate)
#   - Added zoomLevel (required for hourly data to return)
#   - Added per_page, sortBy, sortOrder to get only the newest record
WEATHER_OBS_QUERY = """
query GetWeatherObs(
    $stationId: String!
    $startDate: DateTime!
    $endDate: DateTime!
    $zoomLevel: Int
    $page: Int
    $perPage: Int
    $sortOrder: SortOrder
    $sortBy: WxObsSortBy
) {
    weatherObs(
        stationIds: $stationId
        startDateTimeRange: $startDate
        endDateTimeRange: $endDate
        zoomLevel: $zoomLevel
        page: $page
        per_page: $perPage
        sortBy: $sortBy
        sortOrder: $sortOrder
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

# Shared session for keep-alive (FEMS uses HTTP/2; reusing the conn
# cuts per-request latency significantly).
_session = None

def _get_session():
    global _session
    if _session is None:
        import requests
        _session = requests.Session()
        _session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "FIRESTORM-RAWS-Pipeline/3.0",
            "Origin": "https://fems.fs2c.usda.gov",
            "Referer": "https://fems.fs2c.usda.gov/ui",
        })
    return _session


def gql_request(query, variables, description, timeout=30, verbose_errors=False):
    """
    Fire a single GraphQL request. Returns (parsed_json, rate_limit_headers).
    Logs first-error diagnostic on failures; quiet otherwise to keep
    Actions logs readable at 3000+ station loops.
    """
    session = _get_session()
    payload = {"query": query, "variables": variables}
    try:
        resp = session.post(FEMS_GRAPHQL, json=payload, timeout=timeout)
    except Exception as e:
        if verbose_errors:
            print(f"  {description}: EXCEPTION — {e}")
        return None, {}

    rate_headers = {
        "ratelimit_remaining": _safe_int(resp.headers.get("x-ratelimit-remaining")),
        "slowdown_remaining":  _safe_int(resp.headers.get("x-slowdown-remaining")),
    }

    if resp.status_code != 200:
        if verbose_errors:
            print(f"  {description}: HTTP {resp.status_code}")
            print(f"    Body preview: {resp.text[:300]}")
        return None, rate_headers

    try:
        data = resp.json()
    except ValueError:
        if verbose_errors:
            print(f"  {description}: non-JSON response")
        return None, rate_headers

    if "errors" in data and data.get("errors"):
        if verbose_errors:
            print(f"  {description}: GraphQL errors: "
                  f"{json.dumps(data['errors'])[:300]}")
        return None, rate_headers

    return data, rate_headers


def _safe_int(s):
    try: return int(s)
    except (TypeError, ValueError): return None


# ── Phase 1: Station metadata ────────────────────────────────────────

def fetch_all_stations():
    """Pull the complete station list from FEMS."""
    print("\n[STATIONS] Fetching full RAWS station metadata from FEMS...")

    result, _ = gql_request(
        STATION_LIST_QUERY,
        {"hasHistoricData": "ALL", "weatherHourlyVisible": "ALL"},
        description="All-stations query",
        timeout=60,
        verbose_errors=True,
    )

    if not result or "data" not in result:
        print("  No data key in response — aborting.")
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
    For each station, query the single most recent hourly observation
    from the last 6 hours. Prefer observed ('O') over forecast ('F'),
    fall back to 'F' only if no 'O' exists in the window.

    Uses per_page=5 + sortBy=observationTime + sortOrder=DESC to pull
    only the top few records (enough to find the latest 'O' even if
    the last 1-2 are 'F').
    """
    print(f"\n[WEATHER] Fetching latest obs for {len(stations)} stations...")

    # 6h window is enough: typical RAWS report every hour, and we want
    # the "current conditions" snapshot. Stations offline >6h should
    # legitimately show no recent data.
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(hours=6)
    end_str = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    active_stations = [s for s in stations if s.get("weatherHourly")]
    print(f"  Active hourly-weather stations: {len(active_stations)}")
    print(f"  Window: {start_str} → {end_str}")

    latest_by_station = {}
    total = len(active_stations)
    t_start = time.time()
    first_error_logged = False
    error_count = 0
    current_delay = INTER_REQUEST_DELAY

    for i, station in enumerate(active_stations):
        sid = station["stationId"]

        result, rate = gql_request(
            WEATHER_OBS_QUERY,
            {
                "stationId": sid,
                "startDate": start_str,
                "endDate": end_str,
                "zoomLevel": FEMS_ZOOM_HOURLY,
                "page": 0,
                "perPage": 5,                        # just enough to find latest 'O'
                "sortBy": "observationTime",
                "sortOrder": "DESC",
            },
            description=f"Station {sid}",
            timeout=15,
            # Verbose on the very first failure so we catch any schema drift
            verbose_errors=not first_error_logged,
        )

        if result is None:
            error_count += 1
            if not first_error_logged:
                first_error_logged = True
                print(f"  First error on station {sid} — continuing quietly. "
                      f"Watching for pattern.")
        else:
            payload = result.get("data", {}).get("weatherObs") or {}
            records = payload.get("data", []) or []

            observed = [r for r in records if r.get("observation_type") == "O"]
            pool = observed if observed else records
            if pool:
                # DESC sort means records[0] is newest, but filter first.
                latest = pool[0] if observed else max(
                    pool, key=lambda r: r.get("observation_time", "")
                )
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

        # Adaptive rate-limit backoff based on FEMS headers
        sr = rate.get("slowdown_remaining")
        if sr is not None and sr < SLOWDOWN_THRESHOLD:
            if current_delay != SLOWDOWN_DELAY:
                print(f"  [{i}] slowdown_remaining={sr}, easing pace "
                      f"to {SLOWDOWN_DELAY}s/req")
                current_delay = SLOWDOWN_DELAY
        elif sr is not None and sr > SLOWDOWN_THRESHOLD + 50:
            current_delay = INTER_REQUEST_DELAY

        if (i + 1) % BATCH_STATUS_EVERY == 0:
            elapsed = time.time() - t_start
            rate_per_sec = (i + 1) / elapsed if elapsed > 0 else 0
            eta = (total - i - 1) / rate_per_sec if rate_per_sec > 0 else 0
            print(f"  [{i+1}/{total}] elapsed {elapsed:.0f}s, "
                  f"eta {eta:.0f}s, rate {rate_per_sec:.1f}/s, "
                  f"got {len(latest_by_station)} obs, errors {error_count}")

        time.sleep(current_delay)

    elapsed = time.time() - t_start
    success_rate = (100 * len(latest_by_station) / total) if total else 0
    print(f"\n  ✓ Got latest obs for {len(latest_by_station)}/{total} "
          f"stations ({success_rate:.1f}%) in {elapsed:.0f}s")
    if error_count:
        print(f"  ⚠ {error_count} stations errored out")
    return latest_by_station


# ── Main ─────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("FIRESTORM RAWS Pipeline v3 (FEMS GraphQL, fixed schema)")
    print(f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    stations = fetch_all_stations()
    if not stations:
        print("\nABORT: No stations retrieved.")
        sys.exit(1)

    latest_obs = fetch_latest_observations(stations)

    for s in stations:
        s["latestObs"] = latest_obs.get(s["stationId"])

    stations_path = os.path.join(DATA_DIR, "raws-stations.json")
    with open(stations_path, "w") as f:
        json.dump({
            "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "schema_version": 3,
            "count": len(stations),
            "stations": stations,
        }, f, separators=(",", ":"))
    size_kb = os.path.getsize(stations_path) / 1024
    print(f"\n[OUTPUT] {stations_path} ({size_kb:.0f} KB)")

    latest_path = os.path.join(DATA_DIR, "raws-latest.json")
    with open(latest_path, "w") as f:
        json.dump({
            "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "schema_version": 3,
            "count": len(latest_obs),
            "observations": latest_obs,
        }, f, separators=(",", ":"))
    print(f"[OUTPUT] {latest_path} "
          f"({os.path.getsize(latest_path)/1024:.0f} KB)")

    meta = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "schema_version": 3,
        "counts": {
            "stations_total": len(stations),
            "stations_with_hourly": sum(1 for s in stations if s.get("weatherHourly")),
            "stations_with_latest_obs": len(latest_obs),
            "stations_with_nfdrs": sum(1 for s in stations if s.get("produceNfdrs")),
        },
        "sources": {"fems_graphql": FEMS_GRAPHQL},
        "phase": "Phase 1+2 (stations + latest obs). "
                 "Phase 3 (NFDRS indices) not yet implemented.",
        "pipeline_version": "v3 (fixed weatherObs schema)",
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
