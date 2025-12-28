"""
Notification handlers for scrape job results.

Supports:
- Slack webhooks
- Discord webhooks
- Logging fallback
"""

import logging
from typing import List, Optional, Dict, Any

import httpx

from ..config.settings import settings
from ..harvester.orchestrator import ScrapingResult

logger = logging.getLogger(__name__)


def _escape_slack_markdown(text: str) -> str:
    """
    Escape special Slack markdown characters in user-provided text.

    FIX #53: Company names with *, _, ` can break Slack formatting.
    """
    if not text:
        return text
    # Escape special markdown characters
    # Order matters: escape backtick first to avoid double-escaping
    for char in ('`', '*', '_', '~'):
        text = text.replace(char, f'\\{char}')
    return text


async def send_scrape_summary(
    job_id: str,
    results: List[ScrapingResult],
    duration_seconds: float,
    error: Optional[str] = None,
    external_results: Optional[Dict[str, Any]] = None,
):
    """
    Send scrape summary to configured webhooks.

    Args:
        job_id: Unique identifier for this job run
        results: List of ScrapingResult from each fund
        duration_seconds: Total job duration
        error: Optional error message if job failed
        external_results: Optional dict with external source stats
    """
    # Build summary
    if error:
        summary = _build_error_message(job_id, error, duration_seconds)
    else:
        summary = _build_success_message(job_id, results, duration_seconds, external_results)

    # Log the summary
    logger.info(summary["text"])

    # Send to Slack
    if settings.slack_webhook_url:
        await _send_slack(summary)

    # Send to Discord
    if settings.discord_webhook_url:
        await _send_discord(summary)


def _build_success_message(
    job_id: str,
    results: List[ScrapingResult],
    duration: float,
    external_results: Optional[Dict[str, Any]] = None,
) -> dict:
    """Build success notification message."""
    # Fund website stats
    fund_articles = sum(r.articles_found for r in results)
    fund_deals = sum(r.deals_saved for r in results)
    total_errors = sum(len(r.errors) for r in results)
    funds_with_deals = [r.fund_slug for r in results if r.deals_saved > 0]

    # External source stats
    external_deals = 0
    external_sources = []
    if external_results:
        external_deals = external_results.get("total_deals_saved", 0)
        for source, stats in external_results.items():
            if source != "total_deals_saved" and isinstance(stats, dict):
                deals = stats.get("deals_saved", 0)
                if deals > 0:
                    external_sources.append(f"{source}({deals})")

    total_deals = fund_deals + external_deals
    status_emoji = ":white_check_mark:" if total_errors == 0 else ":warning:"

    text = f"""{status_emoji} *Bud Tracker Scrape Complete* [{job_id}]

*Summary:*
- Funds scraped: {len(results)}
- Fund articles: {fund_articles}
- Fund deals: {fund_deals}
- External deals: {external_deals}
- *Total deals saved: {total_deals}*
- Errors: {total_errors}
- Duration: {duration:.1f}s

*Funds with new deals:* {', '.join(funds_with_deals) or 'None'}
*External sources with deals:* {', '.join(external_sources) or 'None'}"""

    return {
        "text": text.strip(),
        "blocks": [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": text.strip()}
            }
        ]
    }


def _build_error_message(job_id: str, error: str, duration: float) -> dict:
    """Build error notification message."""
    text = f""":x: *Bud Tracker Scrape FAILED* [{job_id}]

*Error:* {error}
*Duration before failure:* {duration:.1f}s

Please check logs for details."""

    return {"text": text.strip()}


async def _send_slack(message: dict) -> bool:
    """Send message to Slack webhook. Returns True on success."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                settings.slack_webhook_url,
                json=message,
                timeout=10.0
            )
            response.raise_for_status()
            logger.info("Slack notification sent")
            return True
    except Exception as e:
        # FIX #19: Log error but return False to track failure
        logger.error(f"Failed to send Slack notification: {e}")
        return False


async def _send_discord(message: dict) -> bool:
    """Send message to Discord webhook. Returns True on success."""
    try:
        # Discord uses 'content' instead of 'text'
        discord_payload = {"content": message["text"]}

        async with httpx.AsyncClient() as client:
            response = await client.post(
                settings.discord_webhook_url,
                json=discord_payload,
                timeout=10.0
            )
            response.raise_for_status()
            logger.info("Discord notification sent")
            return True
    except Exception as e:
        # FIX #19: Log error but return False to track failure
        logger.error(f"Failed to send Discord notification: {e}")
        return False


async def send_lead_deal_alert(
    company_name: str,
    amount: Optional[str],
    round_type: str,
    lead_investor: str,
    enterprise_category: Optional[str],
    verification_snippet: Optional[str],
):
    """
    Send instant alert when a new lead deal is saved.

    Called from storage.py when is_lead_confirmed=True.
    """
    # FIX #53: Escape user-provided fields to prevent Slack markdown issues
    company_name_safe = _escape_slack_markdown(company_name)
    lead_investor_safe = _escape_slack_markdown(lead_investor)
    snippet_safe = _escape_slack_markdown(verification_snippet) if verification_snippet else None

    # Format amount for display
    amount_display = amount if amount else "Undisclosed"
    category_display = enterprise_category.replace("_", " ").title() if enterprise_category else "N/A"
    snippet_display = f'"{snippet_safe}"' if snippet_safe else "N/A"

    text = f"""🚀 *NEW LEAD DEAL DETECTED*

*Company:* {company_name_safe}
*Round:* {round_type.replace("_", " ").title()}
*Amount:* {amount_display}
*Lead:* {lead_investor_safe}
*Category:* {category_display}

*Verification:* {snippet_display}"""

    message = {
        "text": text,
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "🚀 New Lead Deal Detected", "emoji": True}
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Company:*\n{company_name_safe}"},
                    {"type": "mrkdwn", "text": f"*Round:*\n{round_type.replace('_', ' ').title()}"},
                    {"type": "mrkdwn", "text": f"*Amount:*\n{amount_display}"},
                    {"type": "mrkdwn", "text": f"*Lead:*\n{lead_investor_safe}"},
                ]
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Category:*\n{category_display}"},
                ]
            },
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f"*Verification:* {snippet_display}"}
                ]
            }
        ]
    }

    # Log the alert
    logger.info(f"Lead deal alert: {company_name} - {round_type} led by {lead_investor}")

    # FIX #19: Track notification success across channels
    channels_configured = 0
    channels_succeeded = 0

    # Send to Slack
    if settings.slack_webhook_url:
        channels_configured += 1
        if await _send_slack(message):
            channels_succeeded += 1

    # Send to Discord
    if settings.discord_webhook_url:
        channels_configured += 1
        if await _send_discord(message):
            channels_succeeded += 1

    # FIX #19: Log critical error if all configured channels failed
    if channels_configured > 0 and channels_succeeded == 0:
        logger.critical(f"Lead deal alert FAILED: All {channels_configured} notification channels failed for {company_name}")
