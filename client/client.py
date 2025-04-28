import grpc
import json
import requests

from auction_project.server.proto import auction_pb2, auction_pb2_grpc

class AuctionClient:
    def __init__(self, target: str):
        """
        Initialize the client.
        If target starts with http:// or https://, uses JSON-RPC.
        Otherwise, assumes gRPC at host:port.
        """
        self.target = target
        if target.startswith('http://') or target.startswith('https://'):
            self.mode = 'jsonrpc'
        else:
            self.mode = 'grpc'
            self.channel = grpc.insecure_channel(target)
            self.stub = auction_pb2_grpc.AuctionStub(self.channel)

    def create(self, items):
        """
        Start an auction with the given items.
        """
        if self.mode == 'grpc':
            req = auction_pb2.CreateRequest(items=items)
            return self.stub.Create(req)
        else:
            payload = {
                'jsonrpc': '2.0',
                'method': 'create',
                'params': {'items': items},
                'id': 1
            }
            resp = requests.post(self.target, json=payload)
            return resp.json().get('result')

    def bid(self, bidder_id, bundle, value):
        """
        Place a bid on the auction.
        """
        if self.mode == 'grpc':
            req = auction_pb2.BidRequest(bidder=bidder_id, bundle=bundle, value=value)
            return self.stub.Bid(req)
        else:
            payload = {
                'jsonrpc': '2.0',
                'method': 'bid',
                'params': {'bidder': bidder_id, 'bundle': bundle, 'value': value},
                'id': 1
            }
            resp = requests.post(self.target, json=payload)
            return resp.json().get('result')

    def close(self):
        """
        Close the auction.
        """
        if self.mode == 'grpc':
            req = auction_pb2.CloseRequest()
            return self.stub.Close(req)
        else:
            payload = {
                'jsonrpc': '2.0',
                'method': 'close',
                'params': {},
                'id': 1
            }
            resp = requests.post(self.target, json=payload)
            return resp.json().get('result')

    def query(self):
        """
        Query the results of the auction.
        Returns a dict with 'winner' and 'price'.
        """
        if self.mode == 'grpc':
            req = auction_pb2.QueryRequest()
            res = self.stub.Query(req)
            return {'winner': res.winner, 'price': res.price}
        else:
            payload = {
                'jsonrpc': '2.0',
                'method': 'query',
                'params': {},
                'id': 1
            }
            resp = requests.post(self.target, json=payload)
            return resp.json().get('result')
