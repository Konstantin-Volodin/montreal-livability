"""Gold layer contracts: shape, uniqueness, completeness, plus value bounds."""

from dataclasses import dataclass
from typing import Dict, Sequence, Tuple

from montreal.defs.assets.silver.config import POI_CATEGORIES

# Constants shared by the gold assets (livability_score + livability_map).
UNKNOWN_MUNICIPALITY = "Inconnu"
SCORE_COLUMNS = [f"score_{c}" for c in POI_CATEGORIES]

# Default livability blend weights. Single source for both the scoring config
# (LivabilityWeights) and the report's weight display, so the two cannot drift.
DEFAULT_WEIGHTS = {
    "grocery": 0.20,
    "transit": 0.20,
    "park": 0.20,
    "bike": 0.15,
    "school": 0.15,
    "health": 0.10,
}


@dataclass(frozen=True)
class GoldAssetMetadata:
    layer: str
    data_category: str
    segmentation: str   # "snapshot" for a single snapshot, else the shard column
    description: str


@dataclass(frozen=True)
class GoldAssetDataContract:
    """Gold adds value bounds on top of the silver shape checks (scores are 0-100)."""

    schema: Dict[str, str]                       # column -> kind ("numeric"|"str"|"geometry")
    uniqueness: Sequence[str]                    # columns a row must be unique over
    completeness: Sequence[str]                  # columns that must be non-null
    bounds: Dict[str, Tuple[float, float]]       # column -> inclusive (low, high) range
