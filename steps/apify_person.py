"""Step 3: LinkedIn person enrichment via Apify actor."""

import asyncio
import logging
import os
from typing import Callable, List

import httpx

logger = logging.getLogger(__name__)

APIFY_BASE = "https://api.apify.com/v2"
_POLL_INTERVAL = 15       # seconds between status checks
_TIMEOUT = 30 * 60        # 30 minutes


async def check_apify_credits(token: str, notifier: Callable) -> None:
    """Fetch monthly compute-unit usage and warn if < 1 000 units remain."""
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.get(
                f"{APIFY_BASE}/users/me",
                params={"token": token},
            )
            resp.raise_for_status()
            data = resp.json()
            logger.info("Apify /users/me response: %s", data)
            plan = data.get("data", {}).get("plan", {})
            used = plan.get("monthlyUsage", {}).get("ACTOR_COMPUTE_UNITS", 0)
            limit = plan.get("monthlyActorComputeUnits") or plan.get("maxMonthlyActorComputeUnits")
            if limit:
                remaining = max(0, limit - used)
                logger.info("Apify compute units — used: %s, limit: %s, remaining: %s", used, limit, remaining)
                if remaining < 1000:
                    await notifier(
                        f"⚠️ <b>Apify credits low!</b> "
                        f"Only {remaining} compute units remaining this month."
                    )
            else:
                logger.info("Apify compute units used: %s (limit not available in API response)", used)
        except Exception as exc:
            logger.warning("Could not check Apify credits: %s", exc)


async def _start_run(client: httpx.AsyncClient, token: str, actor_id: str, urls: List[str]) -> str:
    """Start an Apify actor run and return the run ID."""
    payload = {
        "queries": urls,
        "profileScraperMode": "Profile details no email ($4 per 1k)",
    }
    resp = await client.post(
        f"{APIFY_BASE}/acts/{actor_id}/runs",
        params={"token": token},
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["data"]["id"]


async def _poll_run(
    client: httpx.AsyncClient,
    token: str,
    run_id: str,
    notifier: Callable = None,
    label: str = "Apify",
) -> dict:
    """Poll until run finishes. Returns the final run data dict."""
    elapsed = 0
    polls = 0
    while elapsed < _TIMEOUT:
        await asyncio.sleep(_POLL_INTERVAL)
        elapsed += _POLL_INTERVAL
        polls += 1
        resp = await client.get(
            f"{APIFY_BASE}/actor-runs/{run_id}",
            params={"token": token},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()["data"]
        status = data["status"]
        logger.info("Apify run %s status: %s", run_id, status)
        if status == "SUCCEEDED":
            return data
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            raise RuntimeError(f"Apify actor run {run_id} ended with status: {status}")
        if notifier and polls % 5 == 0:
            mins = elapsed // 60
            secs = elapsed % 60
            await notifier(f"⏳ {label} still running — {mins}m {secs}s elapsed…")
    raise TimeoutError(f"Apify actor run {run_id} did not finish within 30 minutes")


async def _download_dataset(client: httpx.AsyncClient, token: str, dataset_id: str) -> list:
    resp = await client.get(
        f"{APIFY_BASE}/datasets/{dataset_id}/items",
        params={"token": token, "format": "json"},
        timeout=120,
    )
    resp.raise_for_status()
    return resp.json()


def _map_person(item: dict) -> dict:
    # Full name from firstName + lastName
    first = (item.get("firstName") or "").strip()
    last = (item.get("lastName") or "").strip()
    full_name = f"{first} {last}".strip()

    # Location text from nested dict
    loc = item.get("location", {})
    location_text = (
        loc.get("linkedinText", "") if isinstance(loc, dict) else str(loc)
    )

    # Current position from array
    positions = item.get("currentPosition") or []
    pos = positions[0] if positions else {}
    current_company = pos.get("companyName", "")
    current_title = pos.get("title", "") or item.get("headline", "")
    company_linkedin_url = pos.get("companyLinkedinUrl", "") or pos.get("companyLinkedInUrl", "")

    # Skills — extract names from list of dicts
    raw_skills = item.get("topSkills") or item.get("skills") or []
    if isinstance(raw_skills, list):
        skill_names = [s.get("name", "") if isinstance(s, dict) else str(s) for s in raw_skills]
        skills_text = ", ".join(filter(None, skill_names))
    else:
        skills_text = str(raw_skills)

    # Education — extract school name, degree, field of study
    raw_edu = item.get("profileTopEducation") or item.get("education") or []
    if isinstance(raw_edu, list):
        edu_parts = []
        for e in raw_edu:
            if isinstance(e, dict):
                parts = filter(None, [e.get("schoolName"), e.get("degree"), e.get("fieldOfStudy")])
                edu_parts.append(" | ".join(parts))
        education_text = "; ".join(edu_parts)
    else:
        education_text = str(raw_edu)

    return {
        "linkedin_url": item.get("linkedinUrl") or item.get("profileUrl") or item.get("url", ""),
        "full_name": full_name,
        "headline": item.get("headline", ""),
        "location": location_text,
        "current_company": current_company,
        "current_title": current_title,
        "education": education_text,
        "skills": skills_text,
        "connections_count": item.get("connectionsCount") or item.get("connections", ""),
        "company_linkedin_url": company_linkedin_url,
    }


async def enrich_persons(urls: List[str], notifier: Callable) -> List[dict]:
    token = os.getenv("APIFY_API_TOKEN")
    actor_id = os.getenv("APIFY_ACTOR_PERSON_ID")
    if not token or not actor_id:
        raise ValueError("APIFY_API_TOKEN or APIFY_ACTOR_PERSON_ID not set in .env")

    await check_apify_credits(token, notifier)

    async with httpx.AsyncClient() as client:
        run_id = await _start_run(client, token, actor_id, urls)
        await notifier(
            f"⏳ Apify person enrichment started — run ID: <code>{run_id}</code>\n"
            f"Polling every {_POLL_INTERVAL}s (timeout 30 min)…"
        )

        try:
            run_data = await _poll_run(client, token, run_id, notifier, "Person enrichment")
        except TimeoutError:
            await notifier(
                f"⏱️ <b>Apify person enrichment timed out</b> after 30 minutes.\n"
                f"Run ID: <code>{run_id}</code>\n"
                "Send /resume once the run completes on Apify."
            )
            raise

        dataset_id = run_data["defaultDatasetId"]
        items = await _download_dataset(client, token, dataset_id)

    await check_apify_credits(token, notifier)
    return [_map_person(item) for item in items]
