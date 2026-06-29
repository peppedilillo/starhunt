from conftest import fixture_paths
from conftest import fixture_topic
from conftest import normalized_notice


def notices_by_ivorn():
    return {normalized_notice(path).ivorn: path for path in fixture_paths()}


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
