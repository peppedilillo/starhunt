from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
import json
import math
from typing import Any
from urllib import request

from .utils import is_tz_aware

FINK_ZTF_CONESEARCH_URL = "https://api.ztf.fink-portal.org/api/v1/conesearch"
MAX_CONESEARCH_RADIUS_ARCSEC = 18_000.0  # 5 degrees
MAX_NALERT = 1_000


@dataclass(frozen=True)
class FinkConesearchResult:
    """Fink conesearch response and request metadata.

    Attributes:
        request: JSON payload sent to the Fink conesearch endpoint.
        content: Raw response body bytes.
    """

    request: dict[str, object]
    content: bytes

    def json(self) -> list[dict[str, Any]]:
        data = json.loads(self.content)
        if not isinstance(data, list):
            raise ValueError("Fink conesearch response is not a JSON list")
        if not all(isinstance(item, dict) for item in data):
            raise ValueError("Fink conesearch response rows must be JSON objects")
        return data


def conesearch_fink_ztf(
    ra: float,
    dec: float,
    radius: float,  # in arcseconds
    startdate: datetime,
    stopdate: datetime,
    *,
    max_nalert: int = MAX_NALERT,
    url: str = FINK_ZTF_CONESEARCH_URL,
    timeout: float | None = None,
    opener=request.urlopen,
) -> FinkConesearchResult:
    """
    Query the Fink ZTF conesearch endpoint for alerts in a sky/time window.

    The query is centered on ``ra`` and ``dec`` in degrees, with ``radius`` in
    arcseconds. ``startdate`` and ``stopdate`` must be timezone-aware
    datetimes; they are converted to UTC before being sent to Fink.
    Fink caps conesearch radii at 18,000 arcseconds. Larger requested radii are
    accepted but capped in the outbound request.

    Args:
        ra: Right ascension in degrees, in the inclusive range [0, 360].
        dec: Declination in degrees, in the inclusive range [-90, 90].
        radius: Search radius in arcseconds. Must be positive.
        startdate: Inclusive lower bound for the alert first-detection time.
        stopdate: Exclusive upper bound for the alert first-detection time.
        max_nalert: Maximum number of alerts to return.
        url: Fink conesearch endpoint URL. Intended for tests and alternate deployments.
        timeout: Optional maximum seconds to wait for the HTTP response.
        opener: Callable compatible with ``urllib.request.urlopen``. Intended for tests.

    Returns:
        A ``FinkConesearchResult`` containing the exact request payload and the
        raw response body. Call ``.json()`` on the result to parse and validate
        the response as a list of JSON objects.

    Raises:
        ValueError: If coordinates, radius, or time bounds are invalid.
        urllib.error.URLError: If the HTTP request fails.
    """
    if not is_tz_aware(startdate) or not is_tz_aware(stopdate):
        raise ValueError("startdate and stopdate must be timezone-aware")
    if stopdate <= startdate:
        raise ValueError("stopdate must be later than startdate")
    if not math.isfinite(ra) or not math.isfinite(dec) or not math.isfinite(radius):
        raise ValueError("ra, dec, and radius must be finite")
    if not 0 <= ra <= 360:
        raise ValueError("ra must be between 0 and 360 degrees")
    if not -90 <= dec <= 90:
        raise ValueError("dec must be between -90 and 90 degrees")
    if radius <= 0:
        raise ValueError("radius must be positive")
    if not isinstance(max_nalert, int) or isinstance(max_nalert, bool):
        raise ValueError("max_nalert must be an integer")
    if max_nalert <= 0:
        raise ValueError("max_nalert must be positive")

    startdate_utc = startdate.astimezone(timezone.utc).replace(tzinfo=None).isoformat(sep=" ", timespec="milliseconds")
    stopdate_utc = stopdate.astimezone(timezone.utc).replace(tzinfo=None).isoformat(sep=" ", timespec="milliseconds")

    payload = {
        "ra": ra,
        "dec": dec,
        "radius": min(radius, MAX_CONESEARCH_RADIUS_ARCSEC),
        "startdate": startdate_utc,
        "stopdate": stopdate_utc,
        "n": max_nalert,
        "output-format": "json",
    }
    data = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    http_request = request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    if timeout is None:
        response = opener(http_request)
    else:
        response = opener(http_request, timeout=timeout)
    return FinkConesearchResult(request=payload, content=response.read())
