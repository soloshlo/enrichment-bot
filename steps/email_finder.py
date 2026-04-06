"""Steps 9–10: Look up email addresses via Findymail, then enrich the CSV."""

import asyncio
import logging
import os
import re
from typing import Callable

import httpx
import pandas as pd

logger = logging.getLogger(__name__)

FINDYMAIL_BASE = "https://app.findymail.com/api"
_MAX_RPS = 5  # Findymail rate limit


def _extract_domain(website_url: str) -> str:
    if not isinstance(website_url, str) or not website_url.strip():
        return ""
    domain = re.sub(r"^https?://", "", website_url.strip())
    domain = re.sub(r"^www\.", "", domain)
    domain = domain.split("/")[0].split("?")[0].strip()
    return domain


async def _check_credits(api_key: str, notifier: Callable) -> None:
    headers = {"Authorization": f"Bearer {api_key}"}
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.get(f"{FINDYMAIL_BASE}/credits", headers=headers)
            resp.raise_for_status()
            data = resp.json()
            credits = data.get("credits", "unknown")
            logger.info("Findymail credits remaining: %s", credits)
            try:
                if int(credits) < 50:
                    await notifier(
                        f"⚠️ <b>Findymail credits low!</b> "
                        f"Only {credits} credits remaining."
                    )
            except (TypeError, ValueError):
                pass
        except Exception as exc:
            logger.warning("Could not check Findymail credits: %s", exc)


async def _lookup_one(
    client: httpx.AsyncClient,
    api_key: str,
    idx,
    name: str,
    domain: str,
) -> tuple:
    if not name.strip() or not domain:
        return idx, "Not found"
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        resp = await client.post(
            f"{FINDYMAIL_BASE}/search/name",
            headers=headers,
            json={"name": name.strip(), "domain": domain},
            timeout=30,
        )
        if resp.status_code != 200:
            logger.error("Findymail HTTP %s for %s @ %s: %s", resp.status_code, name, domain, resp.text)
            return idx, "Not found"
        data = resp.json()
        logger.info("Findymail response for %s @ %s: %s", name, domain, data)
        email = data.get("contact", {}).get("email") or "Not found"
        return idx, email or "Not found"
    except Exception as exc:
        logger.error("Findymail error for %s @ %s: %s", name, domain, exc)
        return idx, "Not found"


async def find_emails(
    full_df: pd.DataFrame,
    qualified_df: pd.DataFrame,
    notifier: Callable,
) -> pd.DataFrame:
    api_key = os.getenv("FINDYMAIL_API_KEY")
    if not api_key:
        raise ValueError("FINDYMAIL_API_KEY not set in .env")

    await _check_credits(api_key, notifier)

    full_df = full_df.copy()
    full_df["Email"] = "Not found"

    rows_to_lookup = [
        (
            idx,
            str(qualified_df.loc[idx].get("full_name", "")),
            _extract_domain(str(qualified_df.loc[idx].get("website_url", ""))),
        )
        for idx in qualified_df.index
    ]

    no_domain = sum(1 for _, _, d in rows_to_lookup if not d)
    no_name   = sum(1 for _, n, _ in rows_to_lookup if not n.strip())
    sample = [
        f"{n} @ {d or '(no domain)'}"
        for _, n, d in rows_to_lookup[:3]
    ]
    await notifier(
        f"🔍 <b>Findymail lookup starting</b>\n"
        f"Contacts to search: {len(rows_to_lookup)}\n"
        f"Missing domain: {no_domain} | Missing name: {no_name}\n\n"
        f"First up:\n" + "\n".join(f"• {s}" for s in sample)
    )

    total = len(rows_to_lookup)
    found = 0
    async with httpx.AsyncClient() as client:
        # Process in chunks of _MAX_RPS per second
        for chunk_start in range(0, total, _MAX_RPS):
            chunk = rows_to_lookup[chunk_start: chunk_start + _MAX_RPS]
            tasks = [
                _lookup_one(client, api_key, idx, name, domain)
                for idx, name, domain in chunk
            ]
            results = await asyncio.gather(*tasks)
            for idx, email in results:
                full_df.at[idx, "Email"] = email
                if email and email != "Not found":
                    found += 1

            done = min(chunk_start + _MAX_RPS, total)
            await notifier(f"🔍 Email lookup: {done}/{total} searched — {found} found so far…")

            # Rate-limit pause between chunks
            if chunk_start + _MAX_RPS < total:
                await asyncio.sleep(1)

    await _check_credits(api_key, notifier)
    return full_df
