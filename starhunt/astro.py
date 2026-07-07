from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class Localization:
    """Sky position and error radius, in degrees."""

    ra: float
    dec: float
    err_radius: float
    units: Literal["degrees", "radians"] = "degrees"
