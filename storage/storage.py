import os
import json
import threading

class Storage:
    def __init__(self, log_path: str, snapshot_dir: str):
        """
        Initialize the Storage with paths for the JSONL log and snapshot directory.
        Recovers current index based on existing log length.
        """
        self.log_path = log_path
        self.snapshot_dir = snapshot_dir
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        os.makedirs(snapshot_dir, exist_ok=True)
        self._lock = threading.Lock()
        # Determine current index by counting existing log lines
        self._current_index = 0
        if os.path.exists(self.log_path):
            with open(self.log_path, 'r') as f:
                for _ in f:
                    self._current_index += 1

    def append(self, entry: dict) -> int:
        """
        Atomically write a JSONL record and fsync.
        Returns the entry index.
        """
        with self._lock:
            idx = self._current_index
            # Write to log
            with open(self.log_path, 'a') as f:
                line = json.dumps(entry)
                f.write(line + '\n')
                f.flush()
                os.fsync(f.fileno())
            self._current_index += 1
            return idx

    def snapshot(self, state: dict):
        """
        Write an atomic JSON snapshot of the given state.
        Overwrites the latest_snapshot.json in the snapshot_dir.
        """
        temp_path = os.path.join(self.snapshot_dir, 'snapshot.tmp')
        final_path = os.path.join(self.snapshot_dir, 'latest_snapshot.json')
        # Write to a temp file first
        with open(temp_path, 'w') as f:
            json.dump(state, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, final_path)

    def read_log(self) -> list:
        """
        Recover all log entries from the JSONL log.
        Returns a list of entry dicts.
        """
        entries = []
        if not os.path.exists(self.log_path):
            return entries
        with open(self.log_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                entries.append(json.loads(line))
        return entries

    def read_snapshot(self) -> dict:
        """
        Recover the latest state snapshot.
        Returns the state dict or None if no snapshot exists.
        """
        path = os.path.join(self.snapshot_dir, 'latest_snapshot.json')
        if not os.path.exists(path):
            return None
        with open(path, 'r') as f:
            return json.load(f)
