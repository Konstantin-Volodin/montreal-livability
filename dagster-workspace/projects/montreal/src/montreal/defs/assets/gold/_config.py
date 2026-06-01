"""Gold layer contracts: shape, uniqueness, completeness, plus value bounds."""

from dataclasses import dataclass
from typing import Dict, Sequence, Tuple

from montreal.defs.assets.silver._config import POI_CATEGORIES

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
    """Scores are 0-100 with value bounds on top of silver shape checks."""

    schema: Dict[str, str]
    uniqueness: Sequence[str]
    completeness: Sequence[str]
    bounds: Dict[str, Tuple[float, float]]
