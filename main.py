from pathlib import Path
from storage.storage import Storage
from storage.snapshot_manager import SnapshotManager
from consensus.raft_adapter import RaftNode
from consensus.replicated_log import ReplicatedLog
from auction.state_machine import AuctionServiceCore
from server.grpc_server import serve as serve_auction

# Configuration (could come from CLI)
NODE_ID = "node1"
PEERS   = ["node2:50052", "node3:50053"]
HOST    = "0.0.0.0"
RAFT_PORT    = 6000
AUCTION_PORT = 50051
DATA_DIR = Path("./data")

# Setup storage & snapshots
storage = Storage(str(DATA_DIR/"auction.log"), str(DATA_DIR/"snapshots"))
snap_mgr = SnapshotManager(storage)

# Bootstrap Raft node
def apply_entry(entry):
    svc.state._apply(entry)

raft_node = RaftNode(NODE_ID, PEERS, storage, apply_entry)
raft_server = raft_node.start(HOST, RAFT_PORT)

# Wrap in replicated log
rlog = ReplicatedLog(raft_node)

# Auction service
svc = AuctionServiceCore(rlog)
rlog.register_handler(svc._apply)
rlog.recover()

# Start Auction gRPC API (uses Create/Bid/Close over gRPC)
serve_auction(svc, AUCTION_PORT)
