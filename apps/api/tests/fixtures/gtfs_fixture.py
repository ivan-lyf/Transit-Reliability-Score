"""GTFS test fixture builder - creates in-memory ZIP files for testing."""

from __future__ import annotations

import io
import zipfile

# Minimal valid GTFS data for TransLink-like feed
STOPS_TXT = """\
stop_id,stop_name,stop_lat,stop_lon,stop_code,zone_id
50001,Waterfront Station,49.2856580,-123.1115350,50001,
50002,Burrard Station,49.2855110,-123.1205140,50002,
50003,Granville Station,49.2832750,-123.1161310,50003,
"""

ROUTES_TXT = """\
route_id,agency_id,route_short_name,route_long_name,route_type,route_url
001,TL,1,Commercial-Broadway/Burrard Station,3,
002,TL,2,Macdonald/Downtown,3,
"""

TRIPS_TXT = """\
route_id,service_id,trip_id,trip_headsign,direction_id,block_id,shape_id
001,WD,trip-001-001,Burrard Station,0,block1,shape1
001,WD,trip-001-002,Commercial-Broadway,1,block2,shape2
002,WD,trip-002-001,Downtown,0,block3,shape3
"""

STOP_TIMES_TXT = """\
trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type
trip-001-001,06:30:00,06:30:00,50001,1,0,0
trip-001-001,06:35:00,06:35:00,50002,2,0,0
trip-001-001,06:40:00,06:40:00,50003,3,0,0
trip-001-002,07:00:00,07:00:00,50003,1,0,0
trip-001-002,07:05:00,07:05:00,50002,2,0,0
trip-001-002,07:10:00,07:10:00,50001,3,0,0
trip-002-001,25:01:30,25:01:30,50001,1,0,0
trip-002-001,25:10:00,25:10:00,50002,2,0,0
"""

# Modified version for idempotency testing (changed stop name + added stop)
STOPS_TXT_MODIFIED = """\
stop_id,stop_name,stop_lat,stop_lon,stop_code,zone_id
50001,Waterfront Stn (Renamed),49.2856580,-123.1115350,50001,
50002,Burrard Station,49.2855110,-123.1205140,50002,
50003,Granville Station,49.2832750,-123.1161310,50003,
50004,Stadium-Chinatown Station,49.2794030,-123.1097410,50004,
"""


def build_gtfs_zip(
    stops: str = STOPS_TXT,
    routes: str = ROUTES_TXT,
    trips: str = TRIPS_TXT,
    stop_times: str = STOP_TIMES_TXT,
    extra_files: dict[str, str] | None = None,
    exclude_files: set[str] | None = None,
) -> bytes:
    """Build an in-memory GTFS ZIP file.

    Args:
        stops: Content for stops.txt.
        routes: Content for routes.txt.
        trips: Content for trips.txt.
        stop_times: Content for stop_times.txt.
        extra_files: Additional files to include.
        exclude_files: Files to exclude (e.g. {"stops.txt"}).

    Returns:
        bytes of the ZIP file.
    """
    buf = io.BytesIO()
    exclude = exclude_files or set()

    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        files = {
            "stops.txt": stops,
            "routes.txt": routes,
            "trips.txt": trips,
            "stop_times.txt": stop_times,
        }
        if extra_files:
            files.update(extra_files)

        for name, content in files.items():
            if name not in exclude:
                zf.writestr(name, content)

    return buf.getvalue()


def build_invalid_zip() -> bytes:
    """Build bytes that are not a valid ZIP."""
    return b"This is not a ZIP file at all."


def build_empty_csv_zip() -> bytes:
    """Build a ZIP with empty CSV files (headers only)."""
    return build_gtfs_zip(
        stops="stop_id,stop_name,stop_lat,stop_lon\n",
        routes="route_id,route_short_name,route_long_name\n",
        trips="route_id,service_id,trip_id\n",
        stop_times="trip_id,arrival_time,stop_id,stop_sequence\n",
    )
