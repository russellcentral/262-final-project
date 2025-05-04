import grpc
from grpc import ChannelConnectivity
import json
import threading
import time
import random
import os
import logging

_loglevel = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _loglevel, logging.INFO),
    format='%(asctime)s %(levelname)s %(message)s')

from concurrent import futures

from auction_project.server.proto import raft_pb2, raft_pb2_grpc
from auction_project.storage.storage import Storage
from auction_project.storage.snapshot_manager import SnapshotManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Timeouts in seconds
ELECTION_TIMEOUT_MIN = 1.2
ELECTION_TIMEOUT_MAX = 2.0
HEARTBEAT_INTERVAL = 0.3

class RaftServicer(raft_pb2_grpc.RaftServicer):
    def __init__(self, node):
        self.node = node

    def RequestVote(self, request, context):
        return self.node.handle_request_vote(request)

    def AppendEntries(self, request, context):
        return self.node.handle_append_entries(request)

class RaftNode:
    def __init__(self, node_id: str, all_addresses: list, raft_port: str, data_dir: str, apply_callback):
        self.node_id = node_id
        self.cluster_nodes = list(all_addresses)
        matches = [addr for addr in all_addresses if addr.endswith(f":{raft_port}")]
        
        if not matches:
            raise ValueError(f"No address in {all_addresses} ends with :{raft_port}")
        self.address = matches[0]
        self.peers = [addr for addr in all_addresses if addr != self.address]

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
                pass

        # Recover log entries
        raw = self.storage.read_log()
        # Each entry: {'term': t, 'data': JSON-string}
        self.log = [(e['term'], json.loads(e['data'])) for e in raw]

        # Volatile Raft state
        self.commit_index = -1
        self.last_applied = -1
        self.next_index = {peer: len(self.log) for peer in self.peers}
        self.match_index = {peer: -1 for peer in self.peers}

        from auction_project.consensus.grpc_util import healthy_stub
        def _lazy_stub(peer: str):
            return healthy_stub(peer, raft_pb2_grpc.RaftStub)

        self.stubs = {p: _lazy_stub(p) for p in self.peers}


        # Role and leader tracking
        self.state_lock = threading.Lock()
        self.leader_id = None
        self.role = 'follower'
        self.election_reset_event = threading.Event()
        self.election_reset_event.set()

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
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=40))
        raft_pb2_grpc.add_RaftServicer_to_server(RaftServicer(self), server)
        server.add_insecure_port(f"0.0.0.0:{port}")
        server.start()
        logging.info(f"Node {self.node_id} gRPC Raft service started on 0.0.0.0:{port}")

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
                if self.role != 'leader':
                    self.election_reset_event.clear()
                continue

            # Become candidate
            with self.state_lock:
                self.role = 'candidate'
                self.current_term += 1
                self.voted_for = self.node_id
                self._persist_state()
                term_at_start = self.current_term
            logging.info(f"Node {self.node_id} starts election for term {term_at_start}")

            votes = 1
            for peer in self.peers:
                self._ensure_channel(peer)
                stub = self.stubs[peer]
                req = raft_pb2.RequestVoteRequest(
                    term=term_at_start,
                    candidate_id=self.node_id,
                    last_log_index=len(self.log)-1,
                    last_log_term=self.log[-1][0] if self.log else 0
                )
                try:
                    resp = stub.RequestVote(
                        req,
                        wait_for_ready=True,
                        timeout=3.0
                    )
                    with self.state_lock:
                        # if term out-of-date, step down
                        if resp.term > self.current_term:
                            self.current_term = resp.term
                            self.voted_for = None
                            self.role = 'follower'
                            self._persist_state()
                            logging.info(f"Node {self.node_id} steps down to follower for term {self.current_term}")
                        elif resp.vote_granted:
                            votes += 1
                            self.election_reset_event.set()
                except Exception as e:
                    logging.warning(f"RequestVote to {peer} failed: {e!r}")
                    continue

            # Check majority
            if votes >= ((len(self.peers) + 1) // 2) + 1:
                with self.state_lock:
                    self.role = 'leader'
                    self.leader_id = self.node_id
                    # initialize leader indices
                    for p in self.peers:
                        self.next_index[p] = len(self.log)
                        self.match_index[p] = -1
                    self.election_reset_event.set()
                    logging.info(f"Node {self.node_id} became leader for term {self.current_term}")

    def _on_append_result(self, peer: str, fut: "grpc.Future"):
        """
        Callback for asynchronous AppendEntries RPCs:
            * adjusts term / role if the follower is ahead
            * resets our election timer when the heartbeat succeeds
        """
        try:
            resp = fut.result()
            if resp.term > self.current_term:
                with self.state_lock:
                    self.current_term = resp.term
                    self.role = 'follower'
                    self.voted_for = None
                    self._persist_state()
                    logging.info("%s steps down to follower (higher term %s)",
                                self.node_id, self.current_term)
            elif resp.success:
                self.election_reset_event.set()

        except grpc.RpcError as e:
            logging.debug("AppendEntries → %s failed: %s", peer, e)

    def _ensure_channel(self, peer: str) -> None:
        """
        Guarantee that self.stubs[peer] is backed by a healthy channel
        (READY or IDLE).  If the stub is missing or its channel is in
        TRANSIENT_FAILURE / SHUTDOWN we create a fresh one.

        Works with grpc-python 1.43 → current (≥1.60).
        """

        stub = self.stubs.get(peer)
        if stub is None:
            from auction_project.consensus.grpc_util import healthy_stub
            self.stubs[peer] = healthy_stub(peer, raft_pb2_grpc.RaftStub)
            return
    
        ch = (getattr(stub, "_channel", None) or getattr(stub, "channel", None))

        if ch is None:
            from auction_project.consensus.grpc_util import healthy_stub
            self.stubs[peer] = healthy_stub(peer, raft_pb2_grpc.RaftStub)
            return
        
        state = ch.get_state(try_to_connect=False)
        if state in (
            grpc.ChannelConnectivity.TRANSIENT_FAILURE,
            grpc.ChannelConnectivity.SHUTDOWN,):
            ch.close()
            from auction_project.consensus.grpc_util import healthy_stub
            self.stubs[peer] = healthy_stub(peer, raft_pb2_grpc.RaftStub)
            return

    def _heartbeat_loop(self):
        """
        Runs in its own thread.
        Sends empty AppendEntries (“heartbeat”) to every follower every
        HEARTBEAT_INTERVAL seconds *without blocking* on slow peers.
        """
        while True:
            time.sleep(HEARTBEAT_INTERVAL)

            with self.state_lock:
                if self.role != 'leader':
                    continue
                term   = self.current_term
                commit = self.commit_index

            for peer, stub in self.stubs.items():
                self._ensure_channel(peer)
                stub = self.stubs[peer]
                prev_idx = self.next_index.get(peer, 0) - 1
                prev_term = self.log[prev_idx][0] if prev_idx >= 0 and self.log else 0

                req = raft_pb2.AppendEntriesRequest(
                    term=term,
                    leader_id=self.node_id,
                    prev_log_index=prev_idx,
                    prev_log_term=prev_term,
                    entries=[],
                    leader_commit=commit,
                )
                
                fut = stub.AppendEntries.future(
                    req,
                    wait_for_ready=True,
                    timeout=3.0,
                )
                fut.add_done_callback(
                    lambda f, p=peer: self._on_append_result(p, f))

    def handle_request_vote(self, req):
        """Handle incoming RequestVote RPC."""
        logging.debug(f"{self.node_id}: ↩︎ RequestVote(term={req.term}, from={req.candidate_id})")
        try:
            with self.state_lock:
                if req.term < self.current_term:
                    return raft_pb2.RequestVoteResponse(term=self.current_term, vote_granted=False)
                # Step down if term ahead
                if req.term > self.current_term:
                    self.current_term = req.term
                    self.voted_for = None
                    self.role = 'follower'
                    self._persist_state()
                    logging.info(f"Node {self.node_id} updated to term {self.current_term} as follower")

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
        except Exception:
            logging.exception("RV handler blew up")
            raise 

    def handle_append_entries(self, req):
        """Handle incoming AppendEntries RPC from leader."""
        logging.debug("%s: ↩︎ AppendEntries(term=%s from=%s)",
                  self.node_id, req.term, req.leader_id)
        
        try:
            with self.state_lock:
                if req.term < self.current_term:
                    return raft_pb2.AppendEntriesResponse(term=self.current_term, success=False)

                # Step down and reset election timer
                if req.term > self.current_term:
                    self.current_term = req.term
                    self.voted_for = None
                    self.role = 'follower'
                    self._persist_state()
                    logging.info(f"Node {self.node_id} updated to term {self.current_term} as follower")
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
        except Exception:
            logging.exception("AE handler blew up")
            raise
