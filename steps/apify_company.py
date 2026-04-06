"""Step 5: LinkedIn company enrichment via Apify actor."""

import logging
import os
from typing import Callable, List

import httpx

from steps.apify_person import (
    APIFY_BASE,
    _POLL_INTERVAL,
    _TIMEOUT,
    check_apify_credits,
    _poll_run,
    _download_dataset,
)

logger = logging.getLogger(__name__)


def _map_company(item: dict) -> dict:
    logger.info("Apify company item keys: %s", list(item.keys()))
    return {
        "company_linkedin_url": item.get("linkedInUrl") or item.get("url", ""),
        "company_name": item.get("name") or item.get("companyName", ""),
        "industry": item.get("industry", ""),
        "headcount_range": (
            item.get("staffCount")
            or item.get("headcountRange")
            or item.get("employeeCount", "")
        ),
        "founded_year": item.get("foundedYear") or item.get("founded", ""),
        "description": item.get("description", ""),
        "website_url": item.get("website") or item.get("websiteUrl", ""),
        "specialties": str(item.get("specialties", "")),
        "hq_location": (
            item.get("headquartersCity")
            or item.get("location")
            or item.get("hqLocation", "")
        ),
    }


async def enrich_companies(company_urls: List[str], notifier: Callable) -> List[dict]:
    token = os.getenv("APIFY_API_TOKEN")
    actor_id = os.getenv("APIFY_ACTOR_COMPANY_ID")
    if not token or not actor_id:
        raise ValueError("APIFY_API_TOKEN or APIFY_ACTOR_COMPANY_ID not set in .env")

    await check_apify_credits(token, notifier)

    async with httpx.AsyncClient() as client:
        payload = {
            "profileUrls": company_urls,
        }
        resp = await client.post(
            f"{APIFY_BASE}/acts/{actor_id}/runs",
            params={"token": token},
            json=payload,
            timeout=60,
        )
        resp.raise_for_status()
        run_id = resp.json()["data"]["id"]
        logger.info("Apify company enrichment run started: %s", run_id)
        await notifier(
            f"⏳ Apify company enrichment started — run ID: <code>{run_id}</code>\n"
            f"Polling every {_POLL_INTERVAL}s (timeout 30 min)…"
        )

        try:
            run_data = await _poll_run(client, token, run_id, notifier, "Company enrichment")
        except TimeoutError:
            await notifier(
                f"⏱️ <b>Apify company enrichment timed out</b> after 30 minutes.\n"
                f"Run ID: <code>{run_id}</code>"
            )
            raise

        dataset_id = run_data["defaultDatasetId"]
        items = await _download_dataset(client, token, dataset_id)

    await check_apify_credits(token, notifier)

    # The scraper resolves numeric-ID URLs to slug URLs, breaking the join.
    # Stamp each result with the original input URL so the merger can match correctly.
    mapped = []
    for i, item in enumerate(items):
        record = _map_company(item)
        if i < len(company_urls):
            record["company_linkedin_url"] = company_urls[i]
        mapped.append(record)
    return mapped
