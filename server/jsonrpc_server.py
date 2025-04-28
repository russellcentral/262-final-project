import json
from http.server import BaseHTTPRequestHandler, HTTPServer
import logging

class JSONRPCHandler(BaseHTTPRequestHandler):
    rpc_methods = {}

    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        data = json.loads(self.rfile.read(length))
        method = data.get('method')
        params = data.get('params', {})
        if method in self.rpc_methods:
            try:
                result = self.rpc_methods[method](**params)
                response = {'jsonrpc': '2.0', 'result': result, 'id': data.get('id')}
            except Exception as e:
                response = {'jsonrpc': '2.0', 'error': {'message': str(e)}, 'id': data.get('id')}
        else:
            response = {'jsonrpc': '2.0', 'error': {'message': f'Method {method} not found'}, 'id': data.get('id')}
        resp = json.dumps(response).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(resp)))
        self.end_headers()
        self.wfile.write(resp)

def serve(service, port=8080):
    logging.basicConfig(level=logging.INFO)
    JSONRPCHandler.rpc_methods = {
        'create': lambda items: (service.create(items), {'status': 'ok'})[1],
        'bid': lambda bidder, bundle, value: (service.bid(bidder, bundle, value), {'status': 'ok'})[1],
        'close': lambda: (service.close(), {'status': 'ok'})[1],
        'query': lambda: service.query(),
    }
    server = HTTPServer(('', port), JSONRPCHandler)
    logging.info(f"Starting JSON-RPC server on port {port}")
    server.serve_forever()
