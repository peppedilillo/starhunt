from click.testing import CliRunner

from scripts import consume
from scripts import work


def test_consume_cli_passes_options(monkeypatch, tmp_path):
    calls = []

    def fake_consumer_main(*, output_directory, group_id, offset):
        calls.append(
            {
                "output_directory": output_directory,
                "group_id": group_id,
                "offset": offset,
            }
        )

    monkeypatch.setattr(consume, "run_consumer_main", fake_consumer_main)

    result = CliRunner().invoke(
        consume.main,
        [str(tmp_path), "--group-id", "group-1", "--offset", "latest"],
    )

    assert result.exit_code == 0
    assert calls == [
        {
            "output_directory": tmp_path,
            "group_id": "group-1",
            "offset": "latest",
        }
    ]


def test_consume_cli_rejects_file_output_directory(tmp_path):
    output_file = tmp_path / "output.xml"
    output_file.write_text("<xml />")

    result = CliRunner().invoke(consume.main, [str(output_file)])

    assert result.exit_code != 0
    assert "is a file" in result.output


def test_work_cli_passes_output_directory(monkeypatch, tmp_path):
    calls = []

    def fake_worker_main(*, output_directory, worker_id):
        calls.append(
            {
                "output_directory": output_directory,
                "worker_id": worker_id,
            }
        )

    monkeypatch.setattr(work, "run_worker_main", fake_worker_main)

    result = CliRunner().invoke(work.main, [str(tmp_path)])

    assert result.exit_code == 0
    assert calls == [
        {
            "output_directory": tmp_path,
            "worker_id": None,
        }
    ]


def test_work_cli_passes_worker_id(monkeypatch, tmp_path):
    calls = []

    def fake_worker_main(*, output_directory, worker_id):
        calls.append(
            {
                "output_directory": output_directory,
                "worker_id": worker_id,
            }
        )

    monkeypatch.setattr(work, "run_worker_main", fake_worker_main)

    result = CliRunner().invoke(work.main, [str(tmp_path), "--worker-id", "worker-1"])

    assert result.exit_code == 0
    assert calls == [
        {
            "output_directory": tmp_path,
            "worker_id": "worker-1",
        }
    ]


def test_work_cli_rejects_file_output_directory(tmp_path):
    output_file = tmp_path / "output.json"
    output_file.write_text("{}")

    result = CliRunner().invoke(work.main, [str(output_file)])

    assert result.exit_code != 0
    assert "is a file" in result.output
