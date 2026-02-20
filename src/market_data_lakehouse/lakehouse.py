"""High-throughput market data ingestion and query engine.

Provides columnar storage using Parquet with time-series partitioning,
batch ingestion, OHLCV data model, and efficient time-range queries.
"""
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

# --- Constants ---
DEFAULT_BATCH_SIZE: int = 10_000
MAX_BUFFER_SIZE: int = 1_000_000
PARTITION_FORMAT: str = "%Y-%m-%d"
PARQUET_EXTENSION: str = ".parquet"

# Try importing pyarrow; fall back to CSV-based storage
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False
    logger.warning("pyarrow not installed — using CSV fallback storage")

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


class AssetClass(Enum):
    """Supported asset classes."""
    EQUITY = "equity"
    OPTION = "option"
    FUTURE = "future"
    FOREX = "forex"
    CRYPTO = "crypto"


@dataclass
class OHLCVBar:
    """OHLCV (Open/High/Low/Close/Volume) bar for a single time interval.

    Args:
        symbol: Ticker symbol.
        timestamp: Bar open timestamp.
        open: Opening price.
        high: Highest price in the interval.
        low: Lowest price in the interval.
        close: Closing price.
        volume: Number of shares/contracts traded.
        asset_class: Asset classification.
    """
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    asset_class: AssetClass = AssetClass.EQUITY

    def validate(self) -> None:
        """Validate bar data integrity.

        Raises:
            ValueError: If price relationships or values are invalid.
        """
        if self.high < self.low:
            raise ValueError(
                f"High ({self.high}) must be >= Low ({self.low})"
            )
        if self.open < self.low or self.open > self.high:
            raise ValueError("Open must be between Low and High")
        if self.close < self.low or self.close > self.high:
            raise ValueError("Close must be between Low and High")
        if self.volume < 0:
            raise ValueError("Volume must be non-negative")

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization.

        Returns:
            Dictionary with all bar fields.
        """
        return {
            "symbol": self.symbol,
            "timestamp": self.timestamp.isoformat(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "asset_class": self.asset_class.value,
        }


@dataclass
class QueryResult:
    """Result container for data queries.

    Args:
        bars: List of OHLCV bars matching the query.
        query_time_ms: Time taken to execute the query in milliseconds.
        total_rows_scanned: Number of rows scanned during query.
    """
    bars: list[OHLCVBar]
    query_time_ms: float
    total_rows_scanned: int

    @property
    def count(self) -> int:
        """Number of bars in the result set."""
        return len(self.bars)


@dataclass
class IngestionStats:
    """Statistics from a batch ingestion operation.

    Args:
        rows_ingested: Number of rows written.
        partitions_written: Number of partition files created/appended.
        elapsed_ms: Wall-clock time for the operation.
        errors: Number of validation errors encountered.
    """
    rows_ingested: int
    partitions_written: int
    elapsed_ms: float
    errors: int


class PartitionManager:
    """Manages time-based partitioning of market data on disk.

    Organizes data into date-based partitions: base_path/YYYY-MM-DD/

    Args:
        base_path: Root directory for data storage.
    """

    def __init__(self, base_path: Path) -> None:
        self.base_path = base_path
        self.base_path.mkdir(parents=True, exist_ok=True)

    def partition_key(self, timestamp: datetime) -> str:
        """Derive partition key from a timestamp.

        Args:
            timestamp: The event timestamp.

        Returns:
            Date string used as partition directory name.
        """
        return timestamp.strftime(PARTITION_FORMAT)

    def partition_path(self, partition_key: str) -> Path:
        """Get the filesystem path for a partition.

        Args:
            partition_key: Date string partition identifier.

        Returns:
            Path to the partition directory.
        """
        path = self.base_path / partition_key
        path.mkdir(parents=True, exist_ok=True)
        return path

    def list_partitions(self) -> list[str]:
        """List all existing partition keys sorted chronologically.

        Returns:
            Sorted list of partition key strings.
        """
        if not self.base_path.exists():
            return []
        partitions = [
            d.name for d in self.base_path.iterdir()
            if d.is_dir() and not d.name.startswith(".")
        ]
        return sorted(partitions)

    def partitions_in_range(
        self,
        start: datetime,
        end: datetime,
    ) -> list[str]:
        """Find partitions overlapping a time range.

        Args:
            start: Range start (inclusive).
            end: Range end (inclusive).

        Returns:
            List of partition keys within the range.
        """
        start_key = self.partition_key(start)
        end_key = self.partition_key(end)
        return [
            p for p in self.list_partitions()
            if start_key <= p <= end_key
        ]


class DataLakehouse:
    """High-throughput market data ingestion and query engine.

    Buffers incoming OHLCV bars and flushes them to partitioned
    columnar storage (Parquet when available, CSV fallback).

    Args:
        path: Root directory for the data lake.
        batch_size: Number of records to buffer before auto-flush.
    """

    def __init__(
        self,
        path: str,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self.base_path = Path(path)
        self.batch_size = batch_size
        self.partition_manager = PartitionManager(self.base_path)
        self._buffer: list[OHLCVBar] = []
        self._total_ingested: int = 0
        self._total_errors: int = 0
        logger.info(
            "DataLakehouse initialized at %s (batch_size=%d, pyarrow=%s)",
            self.base_path, self.batch_size, HAS_PYARROW,
        )

    @property
    def buffer_size(self) -> int:
        """Current number of records in the write buffer."""
        return len(self._buffer)

    @property
    def total_ingested(self) -> int:
        """Cumulative count of successfully ingested records."""
        return self._total_ingested

    def ingest(self, bar: OHLCVBar, validate: bool = True) -> bool:
        """Ingest a single OHLCV bar into the write buffer.

        Args:
            bar: The OHLCV bar to ingest.
            validate: Whether to validate bar data before buffering.

        Returns:
            True if the bar was accepted, False on validation failure.
        """
        if validate:
            try:
                bar.validate()
            except ValueError as exc:
                logger.warning("Validation failed for %s: %s", bar.symbol, exc)
                self._total_errors += 1
                return False
        self._buffer.append(bar)
        if len(self._buffer) >= self.batch_size:
            self.flush()
        return True

    def ingest_batch(
        self,
        bars: list[OHLCVBar],
        validate: bool = True,
    ) -> IngestionStats:
        """Ingest a batch of OHLCV bars.

        Args:
            bars: List of bars to ingest.
            validate: Whether to validate each bar.

        Returns:
            IngestionStats summarizing the operation.
        """
        start = time.monotonic()
        accepted = 0
        errors = 0
        for bar in bars:
            if self.ingest(bar, validate=validate):
                accepted += 1
            else:
                errors += 1
        stats = self.flush()
        elapsed = (time.monotonic() - start) * 1000.0
        return IngestionStats(
            rows_ingested=accepted,
            partitions_written=stats.partitions_written,
            elapsed_ms=elapsed,
            errors=errors,
        )

    def flush(self) -> IngestionStats:
        """Flush the write buffer to partitioned storage.

        Returns:
            IngestionStats for this flush operation.
        """
        if not self._buffer:
            return IngestionStats(
                rows_ingested=0, partitions_written=0,
                elapsed_ms=0.0, errors=0,
            )
        start = time.monotonic()
        partitioned = _group_by_partition(self._buffer, self.partition_manager)
        partitions_written = 0
        for partition_key, bars in partitioned.items():
            self._write_partition(partition_key, bars)
            partitions_written += 1
        count = len(self._buffer)
        self._total_ingested += count
        self._buffer = []
        elapsed = (time.monotonic() - start) * 1000.0
        logger.info(
            "Flushed %d records to %d partitions in %.1fms",
            count, partitions_written, elapsed,
        )
        return IngestionStats(
            rows_ingested=count,
            partitions_written=partitions_written,
            elapsed_ms=elapsed,
            errors=0,
        )

    def query(
        self,
        symbol: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> QueryResult:
        """Query stored data with optional filters.

        Args:
            symbol: Filter by ticker symbol (None for all).
            start: Range start (inclusive, None for no lower bound).
            end: Range end (inclusive, None for no upper bound).

        Returns:
            QueryResult with matching bars.
        """
        query_start = time.monotonic()
        partitions = self._resolve_partitions(start, end)
        all_bars: list[OHLCVBar] = []
        total_scanned = 0
        for partition_key in partitions:
            bars, scanned = self._read_partition(partition_key)
            total_scanned += scanned
            all_bars.extend(bars)
        filtered = _filter_bars(all_bars, symbol, start, end)
        elapsed = (time.monotonic() - query_start) * 1000.0
        logger.info(
            "Query returned %d bars (scanned %d) in %.1fms",
            len(filtered), total_scanned, elapsed,
        )
        return QueryResult(
            bars=filtered,
            query_time_ms=elapsed,
            total_rows_scanned=total_scanned,
        )

    def _resolve_partitions(
        self,
        start: Optional[datetime],
        end: Optional[datetime],
    ) -> list[str]:
        """Determine which partitions to scan.

        Args:
            start: Optional start time filter.
            end: Optional end time filter.

        Returns:
            List of partition keys to read.
        """
        if start is not None and end is not None:
            return self.partition_manager.partitions_in_range(start, end)
        return self.partition_manager.list_partitions()

    def _write_partition(
        self,
        partition_key: str,
        bars: list[OHLCVBar],
    ) -> None:
        """Write bars to a partition file.

        Args:
            partition_key: Target partition identifier.
            bars: Bars to write.
        """
        partition_dir = self.partition_manager.partition_path(partition_key)
        filename = f"data_{int(time.time() * 1e6)}"
        if HAS_PYARROW:
            self._write_parquet(partition_dir, filename, bars)
        else:
            self._write_csv(partition_dir, filename, bars)

    def _write_parquet(
        self,
        partition_dir: Path,
        filename: str,
        bars: list[OHLCVBar],
    ) -> None:
        """Write bars as a Parquet file.

        Args:
            partition_dir: Directory to write into.
            filename: Base filename (no extension).
            bars: Bars to serialize.
        """
        filepath = partition_dir / f"{filename}{PARQUET_EXTENSION}"
        table = _bars_to_arrow_table(bars)
        pq.write_table(table, str(filepath))

    def _write_csv(
        self,
        partition_dir: Path,
        filename: str,
        bars: list[OHLCVBar],
    ) -> None:
        """Write bars as a CSV file (fallback).

        Args:
            partition_dir: Directory to write into.
            filename: Base filename (no extension).
            bars: Bars to serialize.
        """
        import csv
        filepath = partition_dir / f"{filename}.csv"
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "symbol", "timestamp", "open", "high",
                    "low", "close", "volume", "asset_class",
                ],
            )
            writer.writeheader()
            for bar in bars:
                writer.writerow(bar.to_dict())

    def _read_partition(
        self,
        partition_key: str,
    ) -> tuple[list[OHLCVBar], int]:
        """Read all data files in a partition.

        Args:
            partition_key: Partition identifier to read.

        Returns:
            Tuple of (bars, rows_scanned).
        """
        partition_dir = self.partition_manager.partition_path(partition_key)
        bars: list[OHLCVBar] = []
        scanned = 0
        for filepath in sorted(partition_dir.iterdir()):
            if filepath.suffix == PARQUET_EXTENSION and HAS_PYARROW:
                partition_bars = _read_parquet_file(filepath)
            elif filepath.suffix == ".csv":
                partition_bars = _read_csv_file(filepath)
            else:
                continue
            scanned += len(partition_bars)
            bars.extend(partition_bars)
        return bars, scanned


# --- Private helpers ---

def _group_by_partition(
    bars: list[OHLCVBar],
    pm: PartitionManager,
) -> dict[str, list[OHLCVBar]]:
    """Group bars by their partition key.

    Args:
        bars: List of bars to partition.
        pm: Partition manager for key derivation.

    Returns:
        Dictionary mapping partition keys to lists of bars.
    """
    groups: dict[str, list[OHLCVBar]] = {}
    for bar in bars:
        key = pm.partition_key(bar.timestamp)
        if key not in groups:
            groups[key] = []
        groups[key].append(bar)
    return groups


def _filter_bars(
    bars: list[OHLCVBar],
    symbol: Optional[str],
    start: Optional[datetime],
    end: Optional[datetime],
) -> list[OHLCVBar]:
    """Apply symbol and time-range filters to a list of bars.

    Args:
        bars: Bars to filter.
        symbol: Optional symbol filter.
        start: Optional start time (inclusive).
        end: Optional end time (inclusive).

    Returns:
        Filtered list of bars, sorted by timestamp.
    """
    filtered = bars
    if symbol is not None:
        filtered = [b for b in filtered if b.symbol == symbol]
    if start is not None:
        filtered = [b for b in filtered if b.timestamp >= start]
    if end is not None:
        filtered = [b for b in filtered if b.timestamp <= end]
    return sorted(filtered, key=lambda b: b.timestamp)


def _bars_to_arrow_table(bars: list[OHLCVBar]) -> "pa.Table":
    """Convert a list of OHLCVBar to a PyArrow Table.

    Args:
        bars: Bars to convert.

    Returns:
        PyArrow Table with typed columns.
    """
    symbols = [b.symbol for b in bars]
    timestamps = [b.timestamp.isoformat() for b in bars]
    opens = [b.open for b in bars]
    highs = [b.high for b in bars]
    lows = [b.low for b in bars]
    closes = [b.close for b in bars]
    volumes = [b.volume for b in bars]
    asset_classes = [b.asset_class.value for b in bars]
    return pa.table({
        "symbol": pa.array(symbols, type=pa.string()),
        "timestamp": pa.array(timestamps, type=pa.string()),
        "open": pa.array(opens, type=pa.float64()),
        "high": pa.array(highs, type=pa.float64()),
        "low": pa.array(lows, type=pa.float64()),
        "close": pa.array(closes, type=pa.float64()),
        "volume": pa.array(volumes, type=pa.int64()),
        "asset_class": pa.array(asset_classes, type=pa.string()),
    })


def _read_parquet_file(filepath: Path) -> list[OHLCVBar]:
    """Read OHLCVBar records from a Parquet file.

    Args:
        filepath: Path to the Parquet file.

    Returns:
        List of deserialized bars.
    """
    table = pq.read_table(str(filepath))
    return _table_to_bars(table)


def _table_to_bars(table: "pa.Table") -> list[OHLCVBar]:
    """Convert a PyArrow Table back to OHLCVBar list.

    Args:
        table: Arrow table with OHLCV columns.

    Returns:
        List of OHLCVBar instances.
    """
    bars: list[OHLCVBar] = []
    for i in range(table.num_rows):
        bar = OHLCVBar(
            symbol=str(table.column("symbol")[i].as_py()),
            timestamp=datetime.fromisoformat(
                str(table.column("timestamp")[i].as_py())
            ),
            open=float(table.column("open")[i].as_py()),
            high=float(table.column("high")[i].as_py()),
            low=float(table.column("low")[i].as_py()),
            close=float(table.column("close")[i].as_py()),
            volume=int(table.column("volume")[i].as_py()),
            asset_class=AssetClass(
                str(table.column("asset_class")[i].as_py())
            ),
        )
        bars.append(bar)
    return bars


def _read_csv_file(filepath: Path) -> list[OHLCVBar]:
    """Read OHLCVBar records from a CSV file.

    Args:
        filepath: Path to the CSV file.

    Returns:
        List of deserialized bars.
    """
    import csv
    bars: list[OHLCVBar] = []
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            bar = OHLCVBar(
                symbol=row["symbol"],
                timestamp=datetime.fromisoformat(row["timestamp"]),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=int(row["volume"]),
                asset_class=AssetClass(row["asset_class"]),
            )
            bars.append(bar)
    return bars
