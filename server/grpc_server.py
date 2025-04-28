import grpc
from concurrent import futures
import logging

from server.proto import auction_pb2, auction_pb2_grpc
from auction.state_machine import AuctionServiceCore

class AuctionServicer(auction_pb2_grpc.AuctionServicer):
    def __init__(self, service: AuctionServiceCore):
        self.service = service

    def Create(self, request, context):
        # Start auction with provided items
        self.service.create(list(request.items))
        return auction_pb2.CreateResponse()

    def Bid(self, request, context):
        # Place a bid
        self.service.bid(request.bidder, list(request.bundle), request.value)
        return auction_pb2.BidResponse()

    def Close(self, request, context):
        # Close the auction
        self.service.close()
        return auction_pb2.CloseResponse()

    def Query(self, request, context):
        # Query the result
        result = self.service.query()
        return auction_pb2.QueryResponse(
            winner=result.get('winner', ""),
            price=result.get('price', 0.0)
        )

def serve(service: AuctionServiceCore, port: int = 50051, max_workers: int = 10):
    logging.basicConfig(level=logging.INFO)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    auction_pb2_grpc.add_AuctionServicer_to_server(
        AuctionServicer(service), server)
    server.add_insecure_port(f'[::]:{port}')
    logging.info(f"Starting Auction gRPC server on port {port}")
    server.start()
    server.wait_for_termination()
