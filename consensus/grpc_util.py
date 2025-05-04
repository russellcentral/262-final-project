import grpc
from   grpc import ChannelConnectivity


def healthy_stub(addr: str, stub_cls):
    """
    Return a stub backed by a channel that transparently
    reconnects (and keeps reconnecting) until it becomes READY.
    """
    ch = grpc.insecure_channel(
        addr,
        options=[
            ('grpc.keepalive_time_ms',       15_000),
            ('grpc.keepalive_timeout_ms',     5_000),
            ('grpc.keepalive_permit_without_calls', 1),
            ('grpc.http2.max_pings_without_data', 0),
            ('grpc.http2.min_time_between_pings_ms', 10_000),
            ('grpc.http2.min_ping_interval_without_data_ms', 10_000),
        ],
    )
    grpc.channel_ready_future(ch).result(timeout=5)
    return stub_cls(ch)
