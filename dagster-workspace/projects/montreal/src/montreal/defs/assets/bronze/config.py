from dataclasses import dataclass
from typing import Dict, Sequence


@dataclass(frozen=True)
class BronzeAssetMetadata:
    layer: str
    data_category: str
    source: str
    description: str
    url: str


@dataclass(frozen=True)
class BronzeAssetDataContract:
    """Every bronze asset must set all four fields (no defaults — a missing one fails at import)."""

    schema: Dict[str, str]        # column -> expected kind ("numeric"|"string"|"geometry")
    uniqueness: Sequence[str]     # columns a row must be unique over
    completeness: Sequence[str]   # columns that must be non-null
    freshness: Dict[str, int]     # e.g. {"max_days": 365}
