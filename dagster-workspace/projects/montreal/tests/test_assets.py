import numpy as np
import pandas as pd
import pytest
import h3

from montreal.defs.assets.gold.analytics import _address_weighted, _distance_score
from montreal.defs.assets.silver.distance import POI_CATEGORIES, haversine, nearest


def test_distance_score_uses_expected_curve():
    scores = _distance_score([0, 100, 500, 1000, 1200, np.nan])

    np.testing.assert_allclose(scores, [100, 100, 50, 20, 0, 0])


def test_address_weighted_uses_address_counts():
    rows = pd.DataFrame(
        {
            "municipality": ["A", "A", "B"],
            "n_addresses": [1, 3, 2],
            "livability": [40.0, 80.0, 10.0],
            **{f"score_{category}": [10.0, 20.0, 30.0] for category in POI_CATEGORIES},
        }
    )

    out = _address_weighted(rows, "municipality").set_index("municipality")

    assert out.loc["A", "addresses"] == 4
    assert out.loc["A", "livability"] == 70
    assert out.loc["B", "addresses"] == 2
    assert out.loc["B", "livability"] == 10


def test_haversine_returns_metres():
    distance = haversine(
        np.array([-73.5673]),
        np.array([45.5017]),
        np.array([-73.5878]),
        np.array([45.5088]),
    )

    assert distance[0] == pytest.approx(1780, rel=0.05)


def test_nearest_resolves_same_cell_category_only():
    lat, lng = 45.5017, -73.5673
    cell = h3.latlng_to_cell(lat, lng, 10)
    addresses = pd.DataFrame({"h3_r10": [cell], "lat": [lat], "lng": [lng]})
    amenities = pd.DataFrame(
        {
            "category": ["grocery"],
            "h3_r10": [cell],
            "lat": [lat],
            "lng": [lng],
        }
    )

    out = nearest(addresses, amenities, max_k=0)

    assert out.loc[0, "dist_grocery"] == pytest.approx(0)
    assert out.loc[0, "dist_school"] is np.nan or np.isnan(out.loc[0, "dist_school"])
