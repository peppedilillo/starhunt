from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class ConeRegion:
    """Circular sky region with coordinates in degrees."""

    ra: float
    dec: float
    err_radius: float
    units: Literal["degrees"] = "degrees"
