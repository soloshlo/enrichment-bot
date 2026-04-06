"""Steps 11–12: Generate four personalised outreach emails per contact via Claude."""

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Callable

import anthropic
import pandas as pd

logger = logging.getLogger(__name__)

_MODEL = "claude-sonnet-4-20250514"
_MAX_TOKENS = 2048
_PROGRESS_EVERY = 10
_BATCH_SIZE = 2  # concurrent Claude calls

EMAIL_COLS = [
    "Subject",
    "Email_1",
    "Email_2",
    "Email_3",
    "Email_4",
]

_SYSTEM_INSTRUCTION = (
    "Return ONLY valid JSON — no markdown fences, no extra text — with this shape:\n"
    '{"subject":"<the one subject line>",'
    '"email_1":"<body of email 1>",'
    '"email_2":"<body of email 2>",'
    '"email_3":"<body of email 3>",'
    '"email_4":"<body of email 4>"}\n'
    "The subject must always start with a capital letter."
)


def _row_to_text(row: pd.Series) -> str:
    skip = set(EMAIL_COLS) | {"Email"}
    return "\n".join(
        f"{k}: {v}"
        for k, v in row.items()
        if k not in skip and pd.notna(v) and str(v).strip() != ""
    )


async def _generate_emails(
    client: anthropic.AsyncAnthropic,
    system: str,
    combined_prompt: str,
    row: pd.Series,
    idx,
) -> tuple:
    """Return (idx, parsed_dict | None, error_str | None)."""
    user_msg = (
        f"{combined_prompt}\n\n"
        f"CONTACT DATA:\n{_row_to_text(row)}\n\n"
        f"{_SYSTEM_INSTRUCTION}"
    )

    for attempt in range(4):
        try:
            response = await client.messages.create(
                model=_MODEL,
                max_tokens=_MAX_TOKENS,
                system=system,
                messages=[{"role": "user", "content": user_msg}],
            )
            raw = response.content[0].text.strip()

            # Strip markdown fences
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            raw = raw.strip()

            parsed = json.loads(raw)
            return idx, parsed, None

        except (json.JSONDecodeError, KeyError) as exc:
            if attempt < 3:
                logger.warning("Row %s email JSON error (attempt %s): %s", idx, attempt + 1, exc)
                await asyncio.sleep(2)
            else:
                logger.error("Row %s: skipping after 4 failed attempts: %s", idx, exc)
                return idx, None, str(exc)

        except anthropic.RateLimitError as exc:
            wait = 60 * (attempt + 1)
            logger.warning("Row %s rate limited (attempt %s), waiting %ss…", idx, attempt + 1, wait)
            await asyncio.sleep(wait)

        except anthropic.APIError as exc:
            logger.error("Row %s Claude API error: %s", idx, exc)
            return idx, None, str(exc)

    return idx, None, "Rate limit: max retries exceeded"


async def write_emails(df: pd.DataFrame, notifier: Callable, save_path: str = None) -> pd.DataFrame:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set in .env")

    system_context = Path("prompts/system_context.txt").read_text(errors="replace")
    prompt_file = os.getenv("EMAIL_PROMPT_FILE", "email_prompt_custdev.txt")
    combined_prompt = Path(f"prompts/{prompt_file}").read_text(errors="replace")

    client = anthropic.AsyncAnthropic(api_key=api_key)
    df = df.copy()

    for col in EMAIL_COLS:
        df[col] = ""

    valid_mask = (
        df["Email"].notna()
        & (df["Email"] != "Not found")
        & (df["Email"].astype(str).str.strip() != "")
    )
    valid_indices = df[valid_mask].index.tolist()
    total = len(valid_indices)
    await notifier(f"✍️ Generating outreach emails for {total} contacts…")

    completed = [0]

    async def _progress_reporter():
        while True:
            await asyncio.sleep(20)
            await notifier(f"✍️ {completed[0]}/{total} leads processed…")

    reporter = asyncio.create_task(_progress_reporter())

    try:
        for batch_start in range(0, total, _BATCH_SIZE):
            batch = valid_indices[batch_start: batch_start + _BATCH_SIZE]
            tasks = [
                _generate_emails(client, system_context, combined_prompt, df.loc[idx], idx)
                for idx in batch
            ]
            results = await asyncio.gather(*tasks)

            for idx, parsed, error in results:
                if parsed:
                    df.at[idx, "Subject"] = parsed.get("subject", "")
                    for n in range(1, 5):
                        df.at[idx, f"Email_{n}"] = parsed.get(f"email_{n}", "")
                    completed[0] += 1
                if error:
                    if "Pipeline_Error" not in df.columns:
                        df["Pipeline_Error"] = ""
                    df.at[idx, "Pipeline_Error"] = f"Email gen: {error}"
                    completed[0] += 1

            if save_path:
                df.to_csv(save_path, index=False)

            if batch_start + _BATCH_SIZE < total:
                await asyncio.sleep(5)
    finally:
        reporter.cancel()

    return df
