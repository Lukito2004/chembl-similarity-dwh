"""Posts pipeline failures to a Teams webhook.

Deliberately free of Airflow imports. A callback receives a plain dictionary, so this
module reads it defensively instead of depending on the scheduler.
"""

from __future__ import annotations

import requests

from chembl_sim.logging_setup import get_logger
from chembl_sim.settings import AlertSettings, get_settings

log = get_logger(__name__)

EXCEPTION_LIMIT = 600

OWNER = "Luka Javakhishvili"
FAILURE_TITLE = f"{OWNER} has upset the pipeline again"

# Served from the public repository, because a card image has to be a URL Teams can fetch
# and a base64 payload of this size would never fit inside the card. The reference is to
# an integration branch rather than a feature branch, so deleting the latter cannot break it.
ALERT_GIF_URL = (
    "https://raw.githubusercontent.com/Lukito2004/chembl-similarity-dwh"
    "/dev/docs/angry-pipeline.gif"
)

# The mood escalates with the retry count, so a first blip and a final failure do not
# look identical in the channel.
MOODS = (
    "Attempt one. Still optimistic.",
    "Attempt two. Optimism is fading.",
    "Attempt three. Nobody is optimistic any more.",
)


def mood_for(try_number: int | None) -> str:
    """Pick a line for how many times this task has already been asked to behave."""
    if not try_number:
        return MOODS[0]
    return MOODS[min(int(try_number), len(MOODS)) - 1]


def describe(context: dict) -> dict:
    """Pull the useful fields out of an Airflow callback context."""
    instance = context.get("task_instance") or context.get("ti")
    return {
        "dag_id": getattr(instance, "dag_id", "unknown"),
        "task_id": getattr(instance, "task_id", "unknown"),
        "run_id": context.get("run_id") or getattr(instance, "run_id", "unknown"),
        "try_number": getattr(instance, "try_number", None),
        "log_url": getattr(instance, "log_url", None),
        "exception": str(context.get("exception") or "").strip()[:EXCEPTION_LIMIT],
    }


def build_message(
    title: str,
    colour: str,
    details: dict,
    image_url: str | None = None,
) -> dict:
    """An adaptive card, plus a plain text field.

    Power Automate flows differ in which part of the body they read, so both shapes are
    supplied rather than guessing at one. An image is attached only when one is given,
    which keeps the success card plain.
    """
    lines = [f"DAG: {details['dag_id']}", f"Task: {details['task_id']}"]
    if details.get("run_id"):
        lines.append(f"Run: {details['run_id']}")
    if details.get("try_number"):
        lines.append(f"Attempt: {details['try_number']}")
    if details.get("exception"):
        lines.append(f"Error: {details['exception']}")
    if details.get("log_url"):
        lines.append(f"Logs: {details['log_url']}")
    body = "\n\n".join(lines)

    blocks = [
        {
            "type": "TextBlock",
            "text": title,
            "weight": "Bolder",
            "size": "Medium",
            "color": colour,
        }
    ]
    if image_url:
        blocks.append(
            {
                "type": "Image",
                "url": image_url,
                # An explicit width renders at the source resolution. size is kept as the
                # fallback for any renderer that ignores width.
                "width": "360px",
                "size": "Large",
                "horizontalAlignment": "Center",
                "altText": "a deeply unimpressed pipeline",
            }
        )
        blocks.append(
            {
                "type": "TextBlock",
                "text": mood_for(details.get("try_number")),
                "isSubtle": True,
                "horizontalAlignment": "Center",
                "wrap": True,
            }
        )
    blocks.append({"type": "TextBlock", "text": body, "wrap": True})
    blocks.append(
        {"type": "TextBlock", "text": f"Reported by {OWNER}", "size": "Small", "isSubtle": True}
    )

    return {
        # Plain text carries the title, for a flow that reads this field instead of the card.
        "text": f"**{title}**\n\n{body}",
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "version": "1.4",
                    "body": blocks,
                },
            }
        ],
    }


def post(message: dict, settings: AlertSettings | None = None) -> bool:
    """Send a message. Never raises, because alerting must not mask the real failure."""
    settings = settings or get_settings().alerts
    if not settings.webhook_url:
        log.warning("No Teams webhook configured, skipping notification")
        return False
    try:
        response = requests.post(
            settings.webhook_url, json=message, timeout=settings.timeout_seconds
        )
    except requests.RequestException as exc:
        log.error("Teams notification failed to send: %s", exc)
        return False
    if response.status_code >= 400:
        log.error("Teams webhook returned HTTP %s: %s", response.status_code, response.text[:200])
        return False
    log.info("Teams notification sent, HTTP %s", response.status_code)
    return True


def notify_failure(context: dict) -> bool:
    """Airflow on_failure_callback. Attach through default_args so every task carries it."""
    details = describe(context)
    log.error("Task failed: %(dag_id)s.%(task_id)s", details)
    return post(build_message(FAILURE_TITLE, "Attention", details, image_url=ALERT_GIF_URL))


def notify_success(context: dict) -> bool:
    """Airflow on_success_callback, used only on the final task of the mart DAG."""
    details = describe(context)
    return post(build_message("Pipeline completed", "Good", details))
