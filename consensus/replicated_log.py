import json
import grpc
from auction_project.server.proto import raft_pb2, raft_pb2_grpc

class ReplicatedLog:
    def __init__(self, raft_node):
        """
        Wraps a RaftNode to provide a simple append() interface.
        raft_node: an instance of RaftNode (leader or follower)
        """
        self.raft = raft_node

    def append(self, entry: dict) -> int:
        """
        Append an entry to the log via Raft consensus.
        Must be called on the leader; otherwise raises.
        Returns the index of the newly appended entry.
        """
        # Only leader can append
        if self.raft.role != 'leader':
            raise RuntimeError("Cannot append on non-leader")

        term = self.raft.current_term
        data_str = json.dumps(entry)

        # Persist to local log storage
        idx = len(self.raft.log)
        # Append to in-memory log
        self.raft.log.append((term, entry))
        # Persist log entry
        self.raft.storage.append({'term': term, 'data': data_str})

        # Initialize leader match_index for self
        # (leader commits its own entries immediately)
        self.raft.match_index[self.raft.node_id] = idx
        self.raft.next_index[self.raft.node_id] = idx + 1

        # Broadcast AppendEntries to followers
        successes = 1  # leader itself
        for peer in self.raft.peers:
            prev_idx = idx - 1
            prev_term = self.raft.log[prev_idx][0] if prev_idx >= 0 else 0
            log_entry = raft_pb2.LogEntry(term=term, data=data_str)
            req = raft_pb2.AppendEntriesRequest(
                term=term,
                leader_id=self.raft.node_id,
                prev_log_index=prev_idx,
                prev_log_term=prev_term,
                entries=[log_entry],
                leader_commit=self.raft.commit_index
            )
            try:
                channel = grpc.insecure_channel(peer)
                stub = raft_pb2_grpc.RaftStub(channel)
                resp = stub.AppendEntries(req)
                # If follower's term is higher, step down
                if resp.term > self.raft.current_term:
                    with self.raft.state_lock:
                        self.raft.current_term = resp.term
                        self.raft.role = 'follower'
                        self.raft.voted_for = None
                        self.raft._persist_state()
                    raise RuntimeError("Stepped down due to higher term")
                if resp.success:
                    successes += 1
                    self.raft.match_index[peer] = idx
                    self.raft.next_index[peer] = idx + 1
                else:
                    # Retry by decrementing next_index
                    self.raft.next_index[peer] = max(0, self.raft.next_index[peer] - 1)
            except Exception:
                # Could not reach follower, skip
                pass

        # Advance commit_index if majority replicated
        N = self.raft.commit_index
        length = len(self.raft.log)
        for new_commit in range(self.raft.commit_index + 1, length):
            # Only commit entries from current term
            if self.raft.log[new_commit][0] != term:
                continue
            count = 0
            for p, match in self.raft.match_index.items():
                if match >= new_commit:
                    count += 1
            # Leader counts as well
            if count >= (len(self.raft.peers) + 1) // 2 + 1:
                N = new_commit
        # Apply entries between commit_index+1 and N
        for apply_idx in range(self.raft.commit_index + 1, N + 1):
            _, ent = self.raft.log[apply_idx]
            self.raft.apply_callback(ent)
            # Optionally snapshot
            try:
                state = self.raft.apply_callback.__self__.state.to_dict()
                self.raft.snapshot_manager.maybe_snapshot(state)
            except Exception:
                pass
        self.raft.commit_index = N
        return idx

    def register_handler(self, handler):
        """
        No-op: RaftNode.apply_callback already handles log application.
        Provided for API compatibility.
        """
        pass
