"""Tests for the market data lakehouse.

Covers OHLCV data model validation, partition management, ingestion,
flushing, querying with time-range filters, and batch operations.
"""
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from market_data_lakehouse.lakehouse import (
    AssetClass,
    DataLakehouse,
    IngestionStats,
    OHLCVBar,
    PartitionManager,
    QueryResult,
)


# --- Fixtures ---

@pytest.fixture
def tmp_lakehouse(tmp_path: Path) -> DataLakehouse:
    """A DataLakehouse backed by a temporary directory."""
    return DataLakehouse(str(tmp_path), batch_size=5)


@pytest.fixture
def sample_bar() -> OHLCVBar:
    """A valid OHLCV bar for AAPL."""
    return OHLCVBar(
        symbol="AAPL",
        timestamp=datetime(2024, 6, 15, 10, 30, 0),
        open=150.0,
        high=155.0,
        low=149.0,
        close=153.0,
        volume=1000000,
        asset_class=AssetClass.EQUITY,
    )


@pytest.fixture
def sample_bars() -> list[OHLCVBar]:
    """Multiple bars across two days for two symbols."""
    base = datetime(2024, 6, 15, 9, 30, 0)
    bars: list[OHLCVBar] = []
    for i in range(10):
        ts = base + timedelta(hours=i)
        bars.append(OHLCVBar(
            symbol="AAPL" if i % 2 == 0 else "GOOGL",
            timestamp=ts,
            open=150.0 + i,
            high=155.0 + i,
            low=149.0 + i,
            close=153.0 + i,
            volume=1000000 + i * 1000,
            asset_class=AssetClass.EQUITY,
        ))
    return bars


# --- OHLCVBar Validation Tests ---

class TestOHLCVBar:
    """Tests for the OHLCV bar data model."""

    def test_valid_bar_passes_validation(self, sample_bar: OHLCVBar) -> None:
        """A well-formed bar passes validation without error."""
        sample_bar.validate()  # Should not raise

    def test_high_less_than_low_raises(self) -> None:
        """Bar with high < low fails validation."""
        bar = OHLCVBar(
            symbol="X", timestamp=datetime.now(),
            open=100.0, high=90.0, low=95.0,
            close=92.0, volume=100,
        )
        with pytest.raises(ValueError, match="High.*Low"):
            bar.validate()

    def test_open_outside_range_raises(self) -> None:
        """Bar with open outside [low, high] fails validation."""
        bar = OHLCVBar(
            symbol="X", timestamp=datetime.now(),
            open=200.0, high=155.0, low=149.0,
            close=150.0, volume=100,
        )
        with pytest.raises(ValueError, match="Open"):
            bar.validate()

    def test_close_outside_range_raises(self) -> None:
        """Bar with close outside [low, high] fails validation."""
        bar = OHLCVBar(
            symbol="X", timestamp=datetime.now(),
            open=150.0, high=155.0, low=149.0,
            close=200.0, volume=100,
        )
        with pytest.raises(ValueError, match="Close"):
            bar.validate()

    def test_negative_volume_raises(self) -> None:
        """Bar with negative volume fails validation."""
        bar = OHLCVBar(
            symbol="X", timestamp=datetime.now(),
            open=150.0, high=155.0, low=149.0,
            close=153.0, volume=-1,
        )
        with pytest.raises(ValueError, match="Volume"):
            bar.validate()

    def test_to_dict_roundtrip(self, sample_bar: OHLCVBar) -> None:
        """to_dict produces expected keys and types."""
        d = sample_bar.to_dict()
        assert d["symbol"] == "AAPL"
        assert d["volume"] == 1000000
        assert d["asset_class"] == "equity"
        assert isinstance(d["timestamp"], str)

    @pytest.mark.parametrize("asset_class", list(AssetClass))
    def test_all_asset_classes(self, asset_class: AssetClass) -> None:
        """Every asset class enum can be used in a bar."""
        bar = OHLCVBar(
            symbol="TEST", timestamp=datetime.now(),
            open=100.0, high=105.0, low=99.0,
            close=102.0, volume=500,
            asset_class=asset_class,
        )
        assert bar.asset_class == asset_class


# --- PartitionManager Tests ---

class TestPartitionManager:
    """Tests for time-based partition management."""

    def test_partition_key_format(self, tmp_path: Path) -> None:
        """Partition key is a date string YYYY-MM-DD."""
        pm = PartitionManager(tmp_path)
        key = pm.partition_key(datetime(2024, 1, 15, 10, 30))
        assert key == "2024-01-15"

    def test_partition_path_creates_dir(self, tmp_path: Path) -> None:
        """partition_path creates the directory if it doesn't exist."""
        pm = PartitionManager(tmp_path)
        path = pm.partition_path("2024-06-15")
        assert path.exists()
        assert path.is_dir()

    def test_list_partitions_empty(self, tmp_path: Path) -> None:
        """No partitions listed for empty directory."""
        pm = PartitionManager(tmp_path)
        assert pm.list_partitions() == []

    def test_partitions_in_range(self, tmp_path: Path) -> None:
        """Filters partitions correctly within a date range."""
        pm = PartitionManager(tmp_path)
        # Create 3 partition directories
        for date_str in ["2024-06-13", "2024-06-14", "2024-06-15", "2024-06-16"]:
            pm.partition_path(date_str)
        result = pm.partitions_in_range(
            datetime(2024, 6, 14), datetime(2024, 6, 15)
        )
        assert result == ["2024-06-14", "2024-06-15"]


# --- Ingestion Tests ---

class TestIngestion:
    """Tests for data ingestion into the lakehouse."""

    def test_ingest_single_bar(
        self, tmp_lakehouse: DataLakehouse, sample_bar: OHLCVBar
    ) -> None:
        """Single bar goes into the buffer."""
        result = tmp_lakehouse.ingest(sample_bar)
        assert result is True
        assert tmp_lakehouse.buffer_size == 1

    def test_ingest_invalid_bar_rejected(
        self, tmp_lakehouse: DataLakehouse
    ) -> None:
        """Invalid bar is rejected with validation enabled."""
        bad_bar = OHLCVBar(
            symbol="X", timestamp=datetime.now(),
            open=200.0, high=100.0, low=90.0,
            close=95.0, volume=100,
        )
        result = tmp_lakehouse.ingest(bad_bar, validate=True)
        assert result is False
        assert tmp_lakehouse.buffer_size == 0

    def test_auto_flush_at_batch_size(
        self, tmp_lakehouse: DataLakehouse
    ) -> None:
        """Buffer auto-flushes when batch_size is reached."""
        base_ts = datetime(2024, 6, 15, 9, 30)
        for i in range(5):
            bar = OHLCVBar(
                symbol="AAPL", timestamp=base_ts + timedelta(minutes=i),
                open=150.0, high=155.0, low=149.0,
                close=153.0, volume=1000,
            )
            tmp_lakehouse.ingest(bar)
        # After 5 bars (batch_size=5), buffer should be flushed
        assert tmp_lakehouse.buffer_size == 0
        assert tmp_lakehouse.total_ingested == 5

    def test_ingest_batch(
        self, tmp_lakehouse: DataLakehouse, sample_bars: list[OHLCVBar]
    ) -> None:
        """Batch ingestion processes all bars and reports stats."""
        stats = tmp_lakehouse.ingest_batch(sample_bars)
        assert isinstance(stats, IngestionStats)
        assert stats.rows_ingested == 10
        assert stats.errors == 0
        assert stats.elapsed_ms > 0


# --- Flush Tests ---

class TestFlush:
    """Tests for flushing buffer to disk."""

    def test_flush_empty_buffer(self, tmp_lakehouse: DataLakehouse) -> None:
        """Flushing an empty buffer returns zero stats."""
        stats = tmp_lakehouse.flush()
        assert stats.rows_ingested == 0
        assert stats.partitions_written == 0

    def test_flush_writes_files(
        self, tmp_lakehouse: DataLakehouse, sample_bar: OHLCVBar
    ) -> None:
        """Flushing creates partition files on disk."""
        tmp_lakehouse.ingest(sample_bar, validate=False)
        stats = tmp_lakehouse.flush()
        assert stats.rows_ingested == 1
        assert stats.partitions_written == 1
        # Verify a partition directory was created
        partitions = tmp_lakehouse.partition_manager.list_partitions()
        assert len(partitions) >= 1


# --- Query Tests ---

class TestQuery:
    """Tests for querying stored data."""

    def test_query_all(
        self, tmp_lakehouse: DataLakehouse, sample_bars: list[OHLCVBar]
    ) -> None:
        """Query without filters returns all ingested data."""
        tmp_lakehouse.ingest_batch(sample_bars, validate=False)
        result = tmp_lakehouse.query()
        assert isinstance(result, QueryResult)
        assert result.count == 10

    def test_query_by_symbol(
        self, tmp_lakehouse: DataLakehouse, sample_bars: list[OHLCVBar]
    ) -> None:
        """Query filtered by symbol returns only matching bars."""
        tmp_lakehouse.ingest_batch(sample_bars, validate=False)
        result = tmp_lakehouse.query(symbol="AAPL")
        # sample_bars has AAPL on even indices: 0, 2, 4, 6, 8 = 5 bars
        assert result.count == 5
        assert all(b.symbol == "AAPL" for b in result.bars)

    def test_query_time_range(
        self, tmp_lakehouse: DataLakehouse, sample_bars: list[OHLCVBar]
    ) -> None:
        """Query with time range filter works correctly."""
        tmp_lakehouse.ingest_batch(sample_bars, validate=False)
        start = datetime(2024, 6, 15, 10, 0, 0)
        end = datetime(2024, 6, 15, 14, 0, 0)
        result = tmp_lakehouse.query(start=start, end=end)
        for bar in result.bars:
            assert bar.timestamp >= start
            assert bar.timestamp <= end

    def test_query_empty_lakehouse(
        self, tmp_lakehouse: DataLakehouse
    ) -> None:
        """Query on empty lakehouse returns zero results."""
        result = tmp_lakehouse.query()
        assert result.count == 0
        assert result.total_rows_scanned == 0

    def test_query_result_sorted_by_time(
        self, tmp_lakehouse: DataLakehouse, sample_bars: list[OHLCVBar]
    ) -> None:
        """Query results are sorted by timestamp."""
        tmp_lakehouse.ingest_batch(sample_bars, validate=False)
        result = tmp_lakehouse.query()
        timestamps = [b.timestamp for b in result.bars]
        assert timestamps == sorted(timestamps)

    def test_query_time_measurement(
        self, tmp_lakehouse: DataLakehouse, sample_bars: list[OHLCVBar]
    ) -> None:
        """Query reports non-negative elapsed time."""
        tmp_lakehouse.ingest_batch(sample_bars, validate=False)
        result = tmp_lakehouse.query()
        assert result.query_time_ms >= 0.0
