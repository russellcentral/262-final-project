import grpc
import json
import threading
import time
import random
import os
from concurrent import futures

from auction_project.server.proto import raft_pb2, raft_pb2_grpc
from auction_project.storage.storage import Storage
from auction_project.storage.snapshot_manager import SnapshotManager

# Timeouts in seconds
ELECTION_TIMEOUT_MIN = 0.15
ELECTION_TIMEOUT_MAX = 0.3
HEARTBEAT_INTERVAL = 0.1

class RaftServicer(raft_pb2_grpc.RaftServicer):
    def __init__(self, node):
        self.node = node

    def RequestVote(self, request, context):
        return self.node.handle_request_vote(request)

    def AppendEntries(self, request, context):
        return self.node.handle_append_entries(request)

class RaftNode:
    def __init__(self, node_id: str, peers: list, data_dir: str, apply_callback):
        self.node_id = node_id
        self.peers = peers  # list of "host:port"

        # Paths for persistent storage and metadata
        self.storage = Storage(f"{data_dir}/raft.log", f"{data_dir}/raft_snapshots")
        self.meta_path = os.path.join(data_dir, 'raft_meta.json')
        self.snapshot_manager = SnapshotManager(self.storage)

        # Callback to apply committed entries to state machine
        self.apply_callback = apply_callback

        # --- Persistent Raft state ---
        # Load metadata if present
        self.current_term = 0
        self.voted_for = None
        if os.path.exists(self.meta_path):
            try:
                with open(self.meta_path, 'r') as mf:
                    meta = json.load(mf)
                self.current_term = meta.get('current_term', 0)
                self.voted_for = meta.get('voted_for', None)
            except Exception:
                self.current_term = 0
                self.voted_for = None

        # Recover log entries
        raw = self.storage.read_log()
        # Each entry: {'term': t, 'data': JSON-string}
        self.log = [(e['term'], json.loads(e['data'])) for e in raw]

        # Volatile Raft state
        self.commit_index = -1
        self.last_applied = -1
        self.next_index = {peer: len(self.log) for peer in peers}
        self.match_index = {peer: -1 for peer in peers}

        # Role and leader tracking
        self.state_lock = threading.Lock()
        self.leader_id = None
        self.role = 'follower'
        self.election_reset_event = threading.Event()

    def _persist_state(self):
        """Persist current_term and voted_for to disk atomically."""
        tmp = self.meta_path + '.tmp'
        with open(tmp, 'w') as mf:
            json.dump({'current_term': self.current_term, 'voted_for': self.voted_for}, mf)
            mf.flush()
            os.fsync(mf.fileno())
        os.replace(tmp, self.meta_path)

    def start(self, host: str, port: int):
        """Start gRPC server and background Raft tasks."""
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftServicer_to_server(RaftServicer(self), server)
        server.add_insecure_port(f"{host}:{port}")
        server.start()

        # Start election and heartbeat loops
        threading.Thread(target=self._election_loop, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        return server

    def _election_loop(self):
        """Election timer: trigger elections on timeout."""
        while True:
            timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
            self.election_reset_event.wait(timeout)
            # Reset by heartbeat or vote
            if self.election_reset_event.is_set():
                self.election_reset_event.clear()
                continue

            # Become candidate
            with self.state_lock:
                self.role = 'candidate'
                self.current_term += 1
                self.voted_for = self.node_id
                self._persist_state()
                term_at_start = self.current_term

            votes = 1
            for peer in self.peers:
                req = raft_pb2.RequestVoteRequest(
                    term=term_at_start,
                    candidate_id=self.node_id,
                    last_log_index=len(self.log)-1,
                    last_log_term=self.log[-1][0] if self.log else 0
                )
                try:
                    channel = grpc.insecure_channel(peer)
                    stub = raft_pb2_grpc.RaftStub(channel)
                    resp = stub.RequestVote(req)
                    with self.state_lock:
                        # if term out-of-date, step down
                        if resp.term > self.current_term:
                            self.current_term = resp.term
                            self.voted_for = None
                            self.role = 'follower'
                            self._persist_state()
                        elif resp.vote_granted:
                            votes += 1
                except Exception:
                    pass

            # Check majority
            if votes > (len(self.peers) + 1) // 2:
                with self.state_lock:
                    self.role = 'leader'
                    self.leader_id = self.node_id
                    # initialize leader indices
                    for p in self.peers:
                        self.next_index[p] = len(self.log)
                        self.match_index[p] = -1

    def _heartbeat_loop(self):
        """Periodic heartbeats from leader."""
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            with self.state_lock:
                if self.role != 'leader':
                    continue
                term = self.current_term
            for peer in self.peers:
                prev_idx = self.next_index.get(peer, 0) - 1
                prev_term = self.log[prev_idx][0] if prev_idx >= 0 and self.log else 0
                req = raft_pb2.AppendEntriesRequest(
                    term=term,
                    leader_id=self.node_id,
                    prev_log_index=prev_idx,
                    prev_log_term=prev_term,
                    entries=[],
                    leader_commit=self.commit_index
                )
                try:
                    channel = grpc.insecure_channel(peer)
                    stub = raft_pb2_grpc.RaftStub(channel)
                    resp = stub.AppendEntries(req)
                    with self.state_lock:
                        if resp.term > self.current_term:
                            self.current_term = resp.term
                            self.voted_for = None
                            self.role = 'follower'
                            self._persist_state()
                        elif resp.success:
                            self.election_reset_event.set()
                except Exception:
                    pass

    def handle_request_vote(self, req):
        """Handle incoming RequestVote RPC."""
        with self.state_lock:
            if req.term < self.current_term:
                return raft_pb2.RequestVoteResponse(term=self.current_term, vote_granted=False)
            # Step down if term ahead
            if req.term > self.current_term:
                self.current_term = req.term
                self.voted_for = None
                self.role = 'follower'
                self._persist_state()

            last_idx = len(self.log) - 1
            last_term = self.log[last_idx][0] if last_idx >= 0 else 0
            can_vote = (self.voted_for is None or self.voted_for == req.candidate_id)
            log_ok = (req.last_log_term > last_term or
                      (req.last_log_term == last_term and req.last_log_index >= last_idx))
            if can_vote and log_ok:
                self.voted_for = req.candidate_id
                self._persist_state()
                self.election_reset_event.set()
                return raft_pb2.RequestVoteResponse(term=self.current_term, vote_granted=True)
            return raft_pb2.RequestVoteResponse(term=self.current_term, vote_granted=False)

    def handle_append_entries(self, req):
        """Handle incoming AppendEntries RPC from leader."""
        with self.state_lock:
            if req.term < self.current_term:
                return raft_pb2.AppendEntriesResponse(term=self.current_term, success=False)

            # Step down and reset election timer
            if req.term > self.current_term:
                self.current_term = req.term
                self.voted_for = None
                self.role = 'follower'
                self._persist_state()
            self.election_reset_event.set()

            # Verify log consistency
            if req.prev_log_index >= 0:
                if req.prev_log_index >= len(self.log) or self.log[req.prev_log_index][0] != req.prev_log_term:
                    return raft_pb2.AppendEntriesResponse(term=self.current_term, success=False)

            # Append new entries
            for entry in req.entries:
                data = json.loads(entry.data)
                idx = len(self.log)
                self.log.append((entry.term, data))
                self.storage.append({'term': entry.term, 'data': entry.data})
                self.commit_index = min(req.leader_commit, len(self.log)-1)

                # Apply newly committed entries
                while self.last_applied < self.commit_index:
                    self.last_applied += 1
                    _, ent = self.log[self.last_applied]
                    self.apply_callback(ent)
                    self.snapshot_manager.maybe_snapshot(self.apply_callback.__self__.state.to_dict())

            return raft_pb2.AppendEntriesResponse(term=self.current_term, success=True)
