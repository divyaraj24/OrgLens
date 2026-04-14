"""
Normalizer unit tests — no external services required.
"""
import pytest
import yaml

from orglens.layers.layer1.models.raw_event import EventType, ReviewVerdict
from orglens.layers.layer1.normalizer.normalizer import Normalizer


# ── Fixture: temp module_map.yaml ─────────────────────────────────────────────

@pytest.fixture
def module_map_file(tmp_path):
    data = {
        "modules": {
            "auth": {"prefixes": ["src/auth/", "django/contrib/auth/"]},
            "orm":  {"prefixes": ["django/db/"]},
            "http": {"prefixes": ["django/http/"]},
            "build": {"prefixes": ["docs/", "scripts/"]},
        },
        "default_module": "other",
    }
    p = tmp_path / "module_map.yaml"
    p.write_text(yaml.dump(data))
    return str(p)


@pytest.fixture
def norm(module_map_file):
    return Normalizer(module_map_path=module_map_file)


# ── Module resolution ─────────────────────────────────────────────────────────

class TestModuleResolution:
    def test_known_prefix(self, norm):
        assert norm.resolve_module("django/contrib/auth/models.py") == "auth"

    def test_second_module(self, norm):
        assert norm.resolve_module("django/db/models/base.py") == "orm"

    def test_fallback(self, norm):
        assert norm.resolve_module("setup.py") == "build"

    def test_partial_prefix_no_match(self, norm):
        # Prefix map does not match; generic multi-layer fallback kicks in.
        assert norm.resolve_module("django/dbus/foo.py") == "django_dbus"

    def test_generic_multilayer_inference(self, norm):
        assert norm.resolve_module("pandas/core/frame.py") == "pandas_core"

    def test_generic_wrapper_skip(self, norm):
        assert norm.resolve_module("src/services/auth/handler.py") == "auth"

    def test_generic_testing_inference(self, norm):
        assert norm.resolve_module("project/tests/test_api.py") == "testing"


# ── Co-author extraction ──────────────────────────────────────────────────────

class TestCoAuthors:
    def test_single(self, norm):
        msg = "Fix bug\n\nCo-authored-by: Alice Smith <alice@example.com>"
        assert norm.extract_co_authors(msg) == ["Alice Smith"]

    def test_multiple(self, norm):
        msg = (
            "Add feature\n\n"
            "Co-authored-by: Bob Jones <bob@example.com>\n"
            "Co-authored-by: Carol Lee <carol@example.com>"
        )
        result = norm.extract_co_authors(msg)
        assert "Bob Jones" in result
        assert "Carol Lee" in result

    def test_none(self, norm):
        assert norm.extract_co_authors("Simple commit message") == []

    def test_case_insensitive(self, norm):
        msg = "co-authored-by: Dave <dave@x.com>"
        assert norm.extract_co_authors(msg) == ["Dave"]


# ── Perceval commit normalization ─────────────────────────────────────────────

def _make_perceval_commit(sha="abc123", files=None, message="Fix issue", co_author=None):
    if files is None:
        files = [{"file": "django/contrib/auth/views.py", "added": "5", "removed": "2"}]
    if co_author:
        message += f"\n\nCo-authored-by: {co_author} <co@example.com>"
    return {
        "data": {
            "commit": sha,
            "Author": "Alice",
            "AuthorEmail": "alice@example.com",
            "AuthorDate": "2024-01-15T10:00:00Z",
            "message": message,
            "files": files,
        }
    }


class TestPercevalCommit:
    def test_basic_event(self, norm):
        item = _make_perceval_commit()
        events = norm.normalize_perceval_commit(item, "django/django")
        assert len(events) == 1
        e = events[0]
        assert e.event_type == EventType.COMMIT
        assert e.actor == "alice"
        assert e.module == "auth"
        assert e.lines_added == 5
        assert e.lines_deleted == 2
        assert e.source == "perceval_git"

    def test_multiple_files(self, norm):
        files = [
            {"file": "django/contrib/auth/forms.py", "added": "3", "removed": "1"},
            {"file": "django/db/models/query.py", "added": "10", "removed": "0"},
        ]
        item = _make_perceval_commit(files=files)
        events = norm.normalize_perceval_commit(item, "repo/x")
        # One event per file
        assert len(events) == 2
        modules = {e.module for e in events}
        assert "auth" in modules
        assert "orm" in modules

    def test_co_author_shadow_events(self, norm):
        item = _make_perceval_commit(co_author="Bob Builder")
        events = norm.normalize_perceval_commit(item, "repo/y")
        # 1 primary + 1 co-author shadow
        assert len(events) == 2
        actors = {e.actor for e in events}
        assert "alice" in actors
        assert "Bob Builder" in actors

    def test_dedup_id_stability(self, norm):
        item = _make_perceval_commit()
        e1 = norm.normalize_perceval_commit(item, "repo")[0]
        e2 = norm.normalize_perceval_commit(item, "repo")[0]
        assert e1.event_id == e2.event_id


# ── Perceval PR normalization ─────────────────────────────────────────────────

def _make_perceval_pr(merged=True, reviews=None):
    data = {
        "number": 42,
        "user": {"login": "carol"},
        "created_at": "2024-02-01T08:00:00Z",
        "merged_at": "2024-02-03T12:00:00Z" if merged else None,
        "merged_by": {"login": "dave"} if merged else None,
        "files": [],
        "requested_reviewers": [{"login": "eve"}],
        "reviews": reviews or [],
    }
    return {"data": data}


class TestPercevalPR:
    def test_pr_open_always(self, norm):
        item = _make_perceval_pr()
        events = norm.normalize_perceval_pr(item, "repo/r")
        types = [e.event_type for e in events]
        assert EventType.PR_OPEN in types

    def test_pr_merge_when_merged(self, norm):
        item = _make_perceval_pr(merged=True)
        events = norm.normalize_perceval_pr(item, "repo/r")
        types = [e.event_type for e in events]
        assert EventType.PR_MERGE in types

    def test_no_merge_event_when_not_merged(self, norm):
        item = _make_perceval_pr(merged=False)
        events = norm.normalize_perceval_pr(item, "repo/r")
        types = [e.event_type for e in events]
        assert EventType.PR_MERGE not in types

    def test_review_events(self, norm):
        reviews = [{"user": {"login": "frank"}, "state": "APPROVED", "submitted_at": "2024-02-02T09:00:00Z"}]
        item = _make_perceval_pr(reviews=reviews)
        events = norm.normalize_perceval_pr(item, "repo/r")
        review_events = [e for e in events if e.event_type == EventType.PR_REVIEW]
        assert len(review_events) == 1
        assert review_events[0].reviewer == "frank"
        assert review_events[0].verdict == ReviewVerdict.APPROVED

    def test_merged_by_captured(self, norm):
        item = _make_perceval_pr(merged=True)
        events = norm.normalize_perceval_pr(item, "repo/r")
        merge_event = next(e for e in events if e.event_type == EventType.PR_MERGE)
        assert merge_event.merged_by == "dave"


# ── Perceval Issue normalization ──────────────────────────────────────────────

def _make_perceval_issue(closed=True):
    return {
        "data": {
            "number": 99,
            "created_at": "2024-03-01T00:00:00Z",
            "closed_at": "2024-03-05T00:00:00Z" if closed else None,
            "labels": [{"name": "bug"}],
            "assignees": [{"login": "grace"}],
            "closed_by": {"login": "henry"} if closed else None,
        }
    }


class TestPercevalIssue:
    def test_assign_event(self, norm):
        item = _make_perceval_issue()
        events = norm.normalize_perceval_issue(item, "repo/i")
        assign_events = [e for e in events if e.event_type == EventType.ISSUE_ASSIGN]
        assert len(assign_events) == 1
        assert assign_events[0].actor == "grace"

    def test_close_event(self, norm):
        item = _make_perceval_issue(closed=True)
        events = norm.normalize_perceval_issue(item, "repo/i")
        close_events = [e for e in events if e.event_type == EventType.ISSUE_CLOSE]
        assert len(close_events) == 1
        assert close_events[0].closer == "henry"

    def test_no_close_when_open(self, norm):
        item = _make_perceval_issue(closed=False)
        events = norm.normalize_perceval_issue(item, "repo/i")
        close_events = [e for e in events if e.event_type == EventType.ISSUE_CLOSE]
        assert len(close_events) == 0

    def test_labels_captured(self, norm):
        item = _make_perceval_issue()
        events = norm.normalize_perceval_issue(item, "repo/i")
        assert all("bug" in e.labels for e in events)


# ── Webhook normalization ─────────────────────────────────────────────────────

class TestWebhookNormalization:
    def _push_payload(self):
        return {
            "repository": {"full_name": "test/repo"},
            "commits": [
                {
                    "id": "deadbeef",
                    "author": {"name": "Ivy", "email": "ivy@example.com"},
                    "timestamp": "2024-04-01T10:00:00Z",
                    "message": "Add auth feature",
                    "added": ["django/contrib/auth/new.py"],
                    "modified": [],
                    "removed": [],
                }
            ],
        }

    def test_push_event(self, norm):
        events = norm.normalize_webhook(self._push_payload(), "push")
        assert len(events) == 1
        assert events[0].event_type == EventType.COMMIT
        assert events[0].module == "auth"
        assert events[0].source == "webhook"

    def test_unsupported_event_returns_empty(self, norm):
        events = norm.normalize_webhook({}, "repository")
        assert events == []

    def test_pr_open_webhook(self, norm):
        payload = {
            "repository": {"full_name": "test/repo"},
            "action": "opened",
            "pull_request": {
                "number": 7,
                "user": {"login": "jack"},
                "created_at": "2024-04-10T08:00:00Z",
                "requested_reviewers": [],
            },
        }
        events = norm.normalize_webhook(payload, "pull_request")
        assert any(e.event_type == EventType.PR_OPEN for e in events)

    def test_pr_review_webhook(self, norm):
        payload = {
            "repository": {"full_name": "test/repo"},
            "review": {
                "user": {"login": "kate"},
                "state": "CHANGES_REQUESTED",
                "submitted_at": "2024-04-11T09:00:00Z",
            },
            "pull_request": {"number": 7},
        }
        events = norm.normalize_webhook(payload, "pull_request_review")
        assert len(events) == 1
        assert events[0].verdict == ReviewVerdict.CHANGES_REQUESTED

    def test_issue_closed_webhook(self, norm):
        payload = {
            "repository": {"full_name": "test/repo"},
            "action": "closed",
            "issue": {
                "number": 55,
                "created_at": "2024-04-01T00:00:00Z",
                "closed_at": "2024-04-05T12:00:00Z",
                "labels": [],
                "assignees": [],
            },
            "sender": {"login": "liam"},
        }
        events = norm.normalize_webhook(payload, "issues")
        assert any(e.event_type == EventType.ISSUE_CLOSE for e in events)
