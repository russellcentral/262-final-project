"""
state_machine.py: Deterministic state machine for auction events
"""
from vcg_auction.common.entries import CreateEntry, BidEntry, CloseEntry
from vcg_auction.auction.allocation import allocate

class AuctionState:
    def __init__(self):
        # Single-item auction state
        self.items = []
        self.bids = []  # List of tuples (bidder_id, value)
        self.closed = False
        self.winner = None
        self.price = None

    def to_dict(self):
        return {
            "items": list(self.items),
            "bids": list(self.bids),
            "closed": self.closed,
            "winner": self.winner,
            "price": self.price,
        }

class AuctionServiceCore:
    def __init__(self, rlog):
        """
        rlog: ReplicatedLog instance for persistence and consensus
        """
        self.rlog = rlog
        self.state = AuctionState()

    def create(self, items):
        """
        Start a new auction for the given items (expecting single-item).
        """
        if self.state.items:
            raise RuntimeError("Auction already created")
        entry = CreateEntry(items=items).to_dict()
        self.rlog.append(entry)

    def bid(self, bidder_id, bundle, value):
        """
        Place a bid on the auction.
        """
        if not self.state.items:
            raise RuntimeError("Auction not created")
        if self.state.closed:
            raise RuntimeError("Auction is closed")
        # In a single-item auction, bundle is ignored
        entry = BidEntry(bidder=bidder_id, bundle=bundle, value=value).to_dict()
        self.rlog.append(entry)

    def close(self):
        """
        Close bidding and determine the winner and price.
        """
        if not self.state.items:
            raise RuntimeError("Auction not created")
        if self.state.closed:
            raise RuntimeError("Auction already closed")
        entry = CloseEntry().to_dict()
        self.rlog.append(entry)

    def query(self):
        """
        Query the current auction result (winner and price).
        If not closed, returns None for both.
        """
        return {"winner": self.state.winner, "price": self.state.price}

    def _apply(self, entry):
        """
        Internal handler to update state based on a log entry.
        """
        etype = entry.get("type")
        if etype == "create":
            e = CreateEntry.from_dict(entry)
            self.state.items = e.items or []
        elif etype == "bid":
            e = BidEntry.from_dict(entry)
            # Record (bidder_id, bid_value)
            self.state.bids.append((e.bidder, e.value))
        elif etype == "close":
            e = CloseEntry.from_dict(entry)
            self.state.closed = True
            # Compute winner and price using second-price allocation
            winner, price = allocate(self.state.bids)
            self.state.winner = winner
            self.state.price = price
        # Ignore unknown entry types
