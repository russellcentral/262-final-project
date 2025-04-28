import os

class SnapshotManager:
    def __init__(self, storage, threshold=1000):
        """
        Manage periodic snapshots of the state.
        storage: Storage instance to handle snapshot writes.
        threshold: number of log entries to trigger a snapshot.
        """
        self.storage = storage
        self.threshold = threshold

    def maybe_snapshot(self, state: dict) -> bool:
        """
        Check log length and trigger snapshot if threshold is reached.
        Returns True if a snapshot was written.
        """
        entries = self.storage.read_log()
        if len(entries) >= self.threshold:
            # Write snapshot
            self.storage.snapshot(state)
            # Optionally, truncate the log to empty (history collapsed)
            # with open(self.storage.log_path, 'w') as f:
            #     pass
            return True
        return False
