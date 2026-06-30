from conftest import fixture_paths
from conftest import fixture_topic
from conftest import normalized_notice
from conftest import parsed_notice

from starhunt.consumer import NoticeVOEvent
from gcn_parser.ep import parse_einstein_probe_wxt


def notices_by_ivorn():
    notices = {}
    for path in fixture_paths():
        notice = normalized_notice(path)
        if isinstance(notice, NoticeVOEvent):
            notices[notice.ivorn] = path
    return notices


def retraction_paths():
    return [path for path in fixture_paths() if normalized_notice(path).retractions]


def test_local_retraction_targets_use_same_topic():
    paths_by_ivorn = notices_by_ivorn()

    for retraction_path in retraction_paths():
        retraction = normalized_notice(retraction_path)
        for target_ivorn in retraction.retractions:
            target_path = paths_by_ivorn.get(target_ivorn)
            if target_path is not None:
                assert fixture_topic(target_path) == fixture_topic(retraction_path)


def test_retractions_do_not_retract_retractions():
    paths_by_ivorn = notices_by_ivorn()

    for retraction_path in retraction_paths():
        retraction = normalized_notice(retraction_path)
        for target_ivorn in retraction.retractions:
            assert not target_ivorn.endswith("_retraction")

            target_path = paths_by_ivorn.get(target_ivorn)
            if target_path is not None:
                assert not normalized_notice(target_path).retractions


def test_retraction_targets_share_svom_burst_id():
    for retraction_path in retraction_paths():
        retraction = normalized_notice(retraction_path)
        for target_ivorn in retraction.retractions:
            assert target_ivorn.rsplit("#", maxsplit=1)[1].startswith(f"{retraction.burst_id}_")


def test_einstein_probe_wxt_notices_have_one_id():
    for path in fixture_paths():
        if fixture_topic(path) == "gcn.notices.einstein_probe.wxt.alert":
            parsed = parsed_notice(path)
            assert len(parsed.id) == 1
            assert normalized_notice(path).burst_id == parsed.id[0]


def test_einstein_probe_wxt_ids_are_unique():
    paths = [
        path
        for path in fixture_paths()
        if fixture_topic(path) == "gcn.notices.einstein_probe.wxt.alert"
    ]
    _burst_ids = [
        id_
        for path in paths
        for id_ in parse_einstein_probe_wxt(path.read_bytes()).id
    ]
    burst_ids = [
        normalized_notice(path).burst_id
        for path in fixture_paths()
        if fixture_topic(path) == "gcn.notices.einstein_probe.wxt.alert"
    ]

    assert len(burst_ids) == len(_burst_ids)
    assert len(burst_ids) == len(set(burst_ids))
