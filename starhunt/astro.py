from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class ConeRegion:
    """Circular sky region with coordinates in degrees."""

    ra: float
    dec: float
    err_radius: float
    units: Literal["degrees"] = "degrees"


def cone_region_from_coordinates(
    ra: float | None,
    dec: float | None,
    err_radius: float | None,
) -> ConeRegion | None:
    """Build a cone region from an optional complete coordinate triple."""
    values = (ra, dec, err_radius)
    if values == (None, None, None):
        return None
    if any(value is None for value in values):
        raise ValueError("Cone region coordinates must be all present or all absent")
    return ConeRegion(ra=ra, dec=dec, err_radius=err_radius)
