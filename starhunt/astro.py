from dataclasses import dataclass


@dataclass(frozen=True)
class Localization:
    """Sky position and error radius, in degrees."""

    ra: float
    dec: float
    err_radius: float
