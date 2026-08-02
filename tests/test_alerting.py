import pytest
import requests
import requests_mock

from chembl_sim import alerting
from chembl_sim.settings import AlertSettings

URL = "https://flow.test/hook"


class FakeInstance:
    dag_id = "similarity_mart"
    task_id = "compute_similarity"
    run_id = "manual__2026-08-01"
    try_number = 2
    log_url = "http://localhost:8080/log"


def context(**overrides):
    base = {"task_instance": FakeInstance(), "run_id": "manual__2026-08-01", "exception": None}
    base.update(overrides)
    return base


@pytest.fixture
def enabled():
    return AlertSettings(webhook_url=URL, timeout_seconds=5)


@pytest.fixture
def disabled():
    return AlertSettings(webhook_url=None, timeout_seconds=5)


def test_the_context_is_read_defensively():
    details = alerting.describe(context(exception=RuntimeError("boom")))
    assert details["dag_id"] == "similarity_mart"
    assert details["task_id"] == "compute_similarity"
    assert details["try_number"] == 2
    assert details["exception"] == "boom"


def test_an_empty_context_does_not_raise():
    details = alerting.describe({})
    assert details["dag_id"] == "unknown"
    assert details["exception"] == ""


def test_a_long_exception_is_truncated():
    details = alerting.describe(context(exception="x" * 5000))
    assert len(details["exception"]) == alerting.EXCEPTION_LIMIT


def test_the_message_carries_both_shapes():
    message = alerting.build_message(
        "Pipeline task failed", "Attention", alerting.describe(context())
    )
    assert "text" in message
    assert message["attachments"][0]["contentType"].endswith("card.adaptive")
    assert message["attachments"][0]["content"]["type"] == "AdaptiveCard"


def test_the_message_names_the_failing_task():
    message = alerting.build_message(
        "Pipeline task failed", "Attention", alerting.describe(context())
    )
    assert "similarity_mart" in message["text"]
    assert "compute_similarity" in message["text"]


def test_nothing_is_sent_without_a_url(disabled, monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("no request may be attempted without a webhook URL")

    monkeypatch.setattr(alerting.requests, "post", refuse)
    assert alerting.post({"text": "hi"}, disabled) is False


def test_a_message_is_posted_as_json(enabled):
    with requests_mock.Mocker() as mocker:
        mocker.post(URL, status_code=202)
        assert alerting.post({"text": "hi"}, enabled) is True
        assert mocker.last_request.json() == {"text": "hi"}


def test_an_http_error_is_swallowed(enabled):
    with requests_mock.Mocker() as mocker:
        mocker.post(URL, status_code=400, text="bad payload")
        assert alerting.post({"text": "hi"}, enabled) is False


def test_a_network_failure_is_swallowed(enabled):
    with requests_mock.Mocker() as mocker:
        mocker.post(URL, exc=requests.exceptions.ConnectTimeout)
        assert alerting.post({"text": "hi"}, enabled) is False


def test_the_failure_callback_posts(enabled, monkeypatch):
    sent = {}
    monkeypatch.setattr(
        alerting, "post", lambda message, settings=None: sent.update(message) or True
    )
    assert alerting.notify_failure(context(exception=ValueError("no fingerprints"))) is True
    assert "no fingerprints" in sent["text"]


def test_the_title_is_not_repeated_inside_the_card():
    message = alerting.build_message(
        "Pipeline task failed", "Attention", alerting.describe(context())
    )
    card_body = message["attachments"][0]["content"]["body"]
    assert card_body[0]["text"] == "Pipeline task failed"
    assert "Pipeline task failed" not in card_body[1]["text"]
    assert message["text"].startswith("**Pipeline task failed**")


def test_the_mood_escalates_with_the_retry_count():
    assert alerting.mood_for(None) == alerting.MOODS[0]
    assert alerting.mood_for(0) == alerting.MOODS[0]
    assert alerting.mood_for(1) == alerting.MOODS[0]
    assert alerting.mood_for(2) == alerting.MOODS[1]
    assert alerting.mood_for(3) == alerting.MOODS[2]
    assert alerting.mood_for(99) == alerting.MOODS[-1]


def test_a_failure_card_carries_the_gif_and_the_owner(enabled):
    with requests_mock.Mocker() as mocker:
        mocker.post(URL, status_code=202)
        alerting.post(
            alerting.build_message(
                alerting.FAILURE_TITLE,
                "Attention",
                alerting.describe(context()),
                image_url=alerting.ALERT_GIF_URL,
            ),
            enabled,
        )
    card = mocker.request_history[0].json()["attachments"][0]["content"]["body"]
    kinds = [block["type"] for block in card]
    assert kinds == ["TextBlock", "Image", "TextBlock", "TextBlock", "TextBlock"]
    assert card[1]["url"] == alerting.ALERT_GIF_URL
    assert card[0]["text"] == alerting.FAILURE_TITLE
    assert alerting.OWNER in card[0]["text"]
    assert card[-1]["text"] == f"Reported by {alerting.OWNER}"


def test_a_success_card_carries_no_image():
    card = alerting.build_message("Pipeline completed", "Good", alerting.describe(context()))
    kinds = [block["type"] for block in card["attachments"][0]["content"]["body"]]
    assert "Image" not in kinds
