"""GTFS ZIP reader - extracts and validates required files."""

from __future__ import annotations

import io
import zipfile

from transit_api.logging import get_logger

logger = get_logger(__name__)

# Files required for our import pipeline
REQUIRED_FILES = {"stops.txt", "routes.txt", "trips.txt", "stop_times.txt"}

# Optional files we can process if present
OPTIONAL_FILES = {"calendar.txt", "calendar_dates.txt"}


class MissingRequiredFileError(Exception):
    """Raised when a required GTFS file is missing from the ZIP."""


class GtfsZipReader:
    """Opens and validates a GTFS ZIP archive."""

    def __init__(self, data: bytes) -> None:
        """Initialize reader with ZIP bytes.

        Raises:
            zipfile.BadZipFile: If data is not a valid ZIP.
            MissingRequiredFileError: If required files are missing.
        """
        self._zip = zipfile.ZipFile(io.BytesIO(data))
        self._validate_required_files()

    def _validate_required_files(self) -> None:
        """Ensure all required GTFS files exist in the archive."""
        names = set(self._zip.namelist())
        missing = REQUIRED_FILES - names
        if missing:
            msg = f"Missing required GTFS files: {sorted(missing)}"
            raise MissingRequiredFileError(msg)

        present_optional = OPTIONAL_FILES & names
        logger.info(
            "GTFS ZIP validated",
            required_files=sorted(REQUIRED_FILES),
            optional_present=sorted(present_optional),
            total_files=len(names),
        )

    def open_file(self, filename: str) -> io.TextIOWrapper:
        """Open a file from the ZIP archive for text reading.

        Returns:
            TextIOWrapper suitable for csv.DictReader.
        """
        binary_stream = self._zip.open(filename)
        return io.TextIOWrapper(binary_stream, encoding="utf-8-sig")

    def list_files(self) -> list[str]:
        """List all filenames in the archive."""
        return self._zip.namelist()

    def close(self) -> None:
        """Close the ZIP archive."""
        self._zip.close()

    def __enter__(self) -> GtfsZipReader:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
