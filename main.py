import argparse
import logging
from pathlib import Path

from auction_project.storage.storage import Storage
from auction_project.storage.snapshot_manager import SnapshotManager
from auction_project.consensus.raft_adapter import RaftNode
from auction_project.consensus.replicated_log import ReplicatedLog
from auction_project.auction.state_machine import AuctionServiceCore
from auction_project.server.grpc_server import serve as serve_grpc
from auction_project.server.jsonrpc_server import serve as serve_jsonrpc

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    parser = argparse.ArgumentParser(description='Auction Project: Raft-backed auction node')
    parser.add_argument('--mode', choices=['grpc', 'jsonrpc'], default='grpc',
                        help='Server mode: gRPC or JSON-RPC (default: grpc)')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind the servers')
    parser.add_argument('--port', type=int, default=50051, help='Port for auction API (default: 50051)')
    parser.add_argument('--raft-port', type=int, default=6000, help='Port for Raft RPCs (default: 6000)')
    parser.add_argument('--data-dir', default='./data', help='Directory for logs, snapshots, and metadata')
    parser.add_argument('--nodes', nargs='+', required=True,
                        help='List of Raft peer addresses (host:port), including self')
    parser.add_argument('--node-id', required=True, help='Unique ID for this Raft node')
    parser.add_argument('--snapshot-threshold', type=int, default=1000,
                        help='Threshold (entries) to trigger snapshots')
    args = parser.parse_args()

    # Prepare data directories
    data_dir = Path(args.data_dir) / args.node_id
    log_path = data_dir / 'raft.log'
    snap_dir = data_dir / 'raft_snapshots'
    data_dir.mkdir(parents=True, exist_ok=True)
    snap_dir.mkdir(parents=True, exist_ok=True)

    # Initialize storage and snapshot manager
    storage = Storage(str(log_path), str(snap_dir))
    snapshot_mgr = SnapshotManager(storage, threshold=args.snapshot_threshold)

    # Placeholder for AuctionServiceCore; rlog will be injected later
    service = AuctionServiceCore(None)

    # Bootstrap Raft node; persist metadata and logs under data_dir
    self_addr = f"{args.host}:{args.raft_port}"
    raft_node = RaftNode(
        node_id=args.node_id,
        all_addresses=args.nodes,
        raft_port=args.raft_port,
        data_dir=str(data_dir),
        apply_callback=lambda entry: service._apply(entry)
    )
    # Start Raft RPC server
    raft_node.start(args.host, args.raft_port)

    # Wrap in replicated log with Raft
    rlog = ReplicatedLog(raft_node)
    # Inject rlog into service, register handler, and recover existing entries
    service.rlog = rlog
    rlog.register_handler(service._apply)
    rlog.recover()

    # Launch Auction API server
    if args.mode == 'grpc':
        serve_grpc(service, port=args.port)
    else:
        serve_jsonrpc(service, port=args.port)

if __name__ == '__main__':
    main()
