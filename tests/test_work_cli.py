from click.testing import CliRunner

from scripts import work


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
