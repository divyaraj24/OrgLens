import pytest

from orglens.pipeline import _FileStore, _parse_args, _parse_repo_url


def test_parse_repo_url_with_git_suffix() -> None:
    parsed = _parse_repo_url("https://github.com/octocat/hello-world.git")
    assert parsed == {
        "owner": "octocat",
        "repo": "hello-world",
        "git_url": "https://github.com/octocat/hello-world.git",
        "repo_key": "octocat/hello-world",
    }


def test_parse_repo_url_without_git_suffix() -> None:
    parsed = _parse_repo_url("https://github.com/octocat/hello-world")
    assert parsed["owner"] == "octocat"
    assert parsed["repo"] == "hello-world"
    assert parsed["git_url"] == "https://github.com/octocat/hello-world.git"


def test_parse_repo_url_invalid() -> None:
    with pytest.raises(ValueError):
        _parse_repo_url("https://github.com/octocat")


def test_parse_args_storage_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "orglens-pipeline",
            "--repo-url",
            "https://github.com/octocat/hello-world.git",
            "--storage-mode",
            "file",
            "--events-file",
            "reports/events.jsonl",
        ],
    )
    args = _parse_args()
    assert args.storage_mode == "file"
    assert args.events_file == "reports/events.jsonl"


@pytest.mark.asyncio
async def test_file_store_deduplicates_events(tmp_path) -> None:
    class _Event:
        def __init__(self, event_id: str, actor: str) -> None:
            self.event_id = event_id
            self.actor = actor

        def model_dump(self, mode: str = "json"):
            return {"event_id": self.event_id, "actor": self.actor}

    store = _FileStore(events_file=str(tmp_path / "events.jsonl"))
    await store.connect()
    written, duplicates = await store.write_events([
        _Event("e1", "alice"),
        _Event("e1", "alice"),
        _Event("e2", "bob"),
    ])
    assert written == 2
    assert duplicates == 1
