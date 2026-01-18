import pandas as pd
from datetime import datetime

class DataLakehouse:
    def __init__(self, path: str):
        self.path = path
        self.buffer: list = []
    
    def ingest(self, event: dict) -> None:
        """Ingest market event."""
        self.buffer.append({**event, 'timestamp': datetime.utcnow()})
    
    def flush(self) -> int:
        """Flush buffer to disk."""
        if not self.buffer:
            return 0
        df = pd.DataFrame(self.buffer)
        df.to_parquet(f"{self.path}/events_{datetime.utcnow().isoformat()}.parquet")
        count = len(self.buffer)
        self.buffer = []
        return count
