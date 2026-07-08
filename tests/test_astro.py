import pytest

from starhunt.astro import cone_region_from_coordinates
from starhunt.astro import ConeRegion


def test_cone_region_from_coordinates_returns_none_for_empty_coordinates():
    assert cone_region_from_coordinates(None, None, None) is None


def test_cone_region_from_coordinates_returns_cone_region_for_complete_coordinates():
    assert cone_region_from_coordinates(1, 2, 0.1) == ConeRegion(ra=1, dec=2, err_radius=0.1)


def test_cone_region_from_coordinates_rejects_partial_coordinates():
    with pytest.raises(ValueError, match="all present or all absent"):
        cone_region_from_coordinates(1, None, 0.1)
