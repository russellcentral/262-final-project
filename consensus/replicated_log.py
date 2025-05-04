import json
import grpc
from auction_project.server.proto import raft_pb2, raft_pb2_grpc

class ReplicatedLog:
    def __init__(self, raft_node):
        """
        Wraps a RaftNode to provide a simple append()/recover() interface.
        raft_node: an instance of RaftNode (leader or follower)
        """
        self.raft = raft_node

    def append(self, entry: dict) -> int:
        """
        Append an entry to the log via Raft consensus.
        Must be called on the leader; otherwise raises.
        Returns the index of the newly appended entry.
        """
        if self.raft.role != 'leader':
            raise RuntimeError("Cannot append on non-leader")

        term = self.raft.current_term
        data_str = json.dumps(entry)

        # Local append
        idx = len(self.raft.log)
        self.raft.log.append((term, entry))
        self.raft.storage.append({'term': term, 'data': data_str})

        # Leader immediately considers its own entry replicated
        self.raft.match_index[self.raft.node_id] = idx
        self.raft.next_index[self.raft.node_id] = idx + 1

        # Send to followers
        for peer in self.raft.peers:
            prev_idx  = idx - 1
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
                stub    = raft_pb2_grpc.RaftStub(channel)
                resp    = stub.AppendEntries(req)
                # Leader steps down if sees higher term
                if resp.term > term:
                    with self.raft.state_lock:
                        self.raft.current_term = resp.term
                        self.raft.role = 'follower'
                        self.raft.voted_for = None
                        self.raft._persist_state()
                    raise RuntimeError("Stepped down due to higher term")
                if resp.success:
                    self.raft.match_index[peer] = idx
                    self.raft.next_index[peer]  = idx + 1
                else:
                    # decrement next_index for retry
                    self.raft.next_index[peer] = max(0, self.raft.next_index[peer] - 1)
            except Exception:
                # ignore unreachable followers
                pass

        # Advance commit_index only on majority
        new_commit = self.raft.commit_index
        for n in range(self.raft.commit_index + 1, len(self.raft.log)):
            # only commit entries from current term
            if self.raft.log[n][0] != term:
                continue
            count = sum(1 for m in self.raft.match_index.values() if m >= n)
            # include leader itself
            if count + 1 >= (len(self.raft.peers) + 1) // 2 + 1:
                new_commit = n

        # Apply any newly committed entries
        for idx_to_apply in range(self.raft.commit_index + 1, new_commit + 1):
            _, ent = self.raft.log[idx_to_apply]
            self.raft.apply_callback(ent)
            try:
                state = self.raft.apply_callback.__self__.state.to_dict()
                self.raft.snapshot_manager.maybe_snapshot(state)
            except Exception:
                pass

        self.raft.commit_index = new_commit
        return idx

    def recover(self):
        """
        Replay all persisted log entries through the state machine.
        """
        for term, entry in self.raft.log:
            self.raft.apply_callback(entry)

    def register_handler(self, handler):
        """
        No-op for compatibility. RaftNode.apply_callback is used instead.
        """
        pass
