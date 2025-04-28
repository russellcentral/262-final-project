from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

@dataclass
class CreateEntry:
    type: str = "create"
    items: List[Any] = None

    def to_dict(self) -> Dict:
        d = asdict(self)
        # Ensure type field is correct
        d["type"] = "create"
        return d

    @staticmethod
    def from_dict(d: Dict) -> 'CreateEntry':
        return CreateEntry(items=d.get("items", []))

@dataclass
class BidEntry:
    type: str = "bid"
    bidder: str = ""
    bundle: List[Any] = None
    value: float = 0.0

    def to_dict(self) -> Dict:
        d = asdict(self)
        d["type"] = "bid"
        return d

    @staticmethod
    def from_dict(d: Dict) -> 'BidEntry':
        return BidEntry(
            bidder=d.get("bidder", ""),
            bundle=d.get("bundle", []),
            value=d.get("value", 0.0)
        )

@dataclass
class CloseEntry:
    type: str = "close"

    def to_dict(self) -> Dict:
        return {"type": "close"}

    @staticmethod
    def from_dict(d: Dict) -> 'CloseEntry':
        return CloseEntry()
