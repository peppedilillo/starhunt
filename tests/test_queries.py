from datetime import datetime
from datetime import timedelta
from datetime import timezone
import json
from pathlib import Path

import pytest

from starhunt.queries import conesearch_fink_ztf

UTC = timezone.utc


class FakeResponse:
    def __init__(self, content: bytes):
        self.content = content

    def read(self):
        return self.content


class FakeOpener:
    def __init__(self, content: bytes = b"[]"):
        self.content = content
        self.request = None

    def __call__(self, request):
        self.request = request
        return FakeResponse(self.content)

    def payload(self):
        return json.loads(self.request.data)


def test_conesearch_rejects_timezone_naive_dates():
    aware = datetime(2026, 1, 1, tzinfo=UTC)
    naive = datetime(2026, 1, 2)

    with pytest.raises(ValueError, match="timezone-aware"):
        conesearch_fink_ztf(
            ra=1.0, dec=2.0, radius=3.0, startdate=aware, stopdate=naive
        )


def test_conesearch_requires_stopdate_after_startdate():
    start = datetime(2026, 1, 1, tzinfo=UTC)

    with pytest.raises(ValueError, match="stopdate"):
        conesearch_fink_ztf(
            ra=1.0,
            dec=2.0,
            radius=3.0,
            startdate=start,
            stopdate=start,
        )


def test_conesearch_caps_radius_and_uses_stopdate():
    opener = FakeOpener()

    result = conesearch_fink_ztf(
        ra=193.822,
        dec=2.89732,
        radius=20_000,
        startdate=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        stopdate=datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
        opener=opener,
    )

    assert result.request == opener.payload()
    assert (
        opener.request.full_url == "https://api.ztf.fink-portal.org/api/v1/conesearch"
    )
    assert opener.request.method == "POST"
    assert opener.request.headers["Content-type"] == "application/json"
    assert result.request["ra"] == 193.822
    assert result.request["dec"] == 2.89732
    assert result.request["radius"] == 18_000
    assert result.request["startdate"] == "2026-01-01 00:00:00.000"
    assert result.request["stopdate"] == "2026-01-01 12:00:00.000"
    assert result.request["n"] == 1_000
    assert "window" not in result.request
    assert result.request["output-format"] == "json"


def test_conesearch_formats_dates_in_utc():
    opener = FakeOpener()

    conesearch_fink_ztf(
        ra=193.822,
        dec=2.89732,
        radius=5,
        startdate=datetime(2026, 1, 1, 1, 0, tzinfo=timezone(timedelta(hours=1))),
        stopdate=datetime(2026, 1, 2, 1, 0, tzinfo=timezone(timedelta(hours=1))),
        opener=opener,
    )

    assert opener.payload()["startdate"] == "2026-01-01 00:00:00.000"
    assert opener.payload()["stopdate"] == "2026-01-02 00:00:00.000"


@pytest.mark.parametrize("max_nalert", [0, -1, 1.5, True])
def test_conesearch_rejects_invalid_max_nalert(max_nalert):
    with pytest.raises(ValueError, match="max_nalert"):
        conesearch_fink_ztf(
            ra=193.822,
            dec=2.89732,
            radius=5,
            startdate=datetime(2026, 1, 1, tzinfo=UTC),
            stopdate=datetime(2026, 1, 2, tzinfo=UTC),
            max_nalert=max_nalert,
        )


def test_conesearch_result_preserves_raw_bytes_and_parses_json():
    content = (
        Path(__file__).parent / "fixtures" / "conesearches" / "sample.json"
    ).read_bytes()

    result = conesearch_fink_ztf(
        ra=193.822,
        dec=2.89732,
        radius=5,
        startdate=datetime(2026, 1, 1, tzinfo=UTC),
        stopdate=datetime(2026, 1, 2, tzinfo=UTC),
        opener=FakeOpener(content),
    )

    assert result.content == content
    assert result.json()[0]["i:objectId"] == "ZTF21abfmbix"


def test_conesearch_result_rejects_non_list_json():
    result = conesearch_fink_ztf(
        ra=193.822,
        dec=2.89732,
        radius=5,
        startdate=datetime(2026, 1, 1, tzinfo=UTC),
        stopdate=datetime(2026, 1, 2, tzinfo=UTC),
        opener=FakeOpener(b'{"not":"a list"}'),
    )

    with pytest.raises(ValueError, match="JSON list"):
        result.json()


@pytest.mark.smoke
def test_conesearch_fink_ztf_live_api_smoke():
    result = conesearch_fink_ztf(
        ra=193.822,
        dec=2.89732,
        radius=5,
        startdate=datetime(2021, 6, 10, 5, 59, 37, tzinfo=UTC),
        stopdate=datetime(2021, 6, 17, 5, 59, 37, tzinfo=UTC),
        max_nalert=1,
    )

    rows = result.json()

    assert isinstance(rows, list)
    assert len(rows) == 1
    assert isinstance(rows[0], dict)
