"""
allocation.py: Winner-determination + second-price auction logic
"""
from typing import List, Tuple, Optional

def allocate(bids: List[Tuple[str, float]]) -> Tuple[Optional[str], Optional[float]]:
    """
    Perform a second-price (Vickrey) auction on a single item.
    bids: list of (bidder_id, bid_value)
    Returns a tuple (winner_id, price), where price is the second-highest bid,
    or 0.0 if there is only one bid, or (None, None) if no bids.
    """
    if not bids:
        return None, None
    # Sort bids by value descending
    sorted_bids = sorted(bids, key=lambda x: x[1], reverse=True)
    winner, highest = sorted_bids[0]
    # Determine price
    if len(sorted_bids) > 1:
        _, second = sorted_bids[1]
        price = second
    else:
        price = 0.0
    return winner, price
