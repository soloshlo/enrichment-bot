"""Step 8: Score every row against the ICP using Claude."""

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
_MAX_TOKENS = 256
_PROGRESS_EVERY = 10


def _row_to_text(row: pd.Series) -> str:
    return "\n".join(
        f"{k}: {v}"
        for k, v in row.items()
        if pd.notna(v) and str(v).strip() != ""
    )


async def _score_row(
    client: anthropic.AsyncAnthropic,
    system: str,
    ranking_prompt: str,
    row: pd.Series,
    idx,
) -> tuple:
    """Return (idx, score, reason).  Retries once on JSON parse failure."""
    user_msg = f"{ranking_prompt}\n\nLEAD DATA:\n{_row_to_text(row)}"

    for attempt in range(5):
        try:
            response = await client.messages.create(
                model=_MODEL,
                max_tokens=_MAX_TOKENS,
                system=system,
                messages=[{"role": "user", "content": user_msg}],
            )
            raw = response.content[0].text.strip()

            # Strip markdown fences if present
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            raw = raw.strip()

            parsed = json.loads(raw)
            score = max(0, min(100, int(parsed["score"])))
            reason = str(parsed.get("reason", ""))
            return idx, score, reason, None

        except (json.JSONDecodeError, KeyError, ValueError) as exc:
            if attempt < 4:
                logger.warning("Row %s JSON parse error (attempt %s): %s", idx, attempt + 1, exc)
                await asyncio.sleep(2)
            else:
                logger.error("Row %s: skipping after 5 failed attempts: %s", idx, exc)
                return idx, 0, "Parse error", str(exc)

        except anthropic.RateLimitError as exc:
            wait = 60 * (attempt + 1)
            logger.warning("Row %s rate limited (attempt %s), waiting %ss…", idx, attempt + 1, wait)
            await asyncio.sleep(wait)

        except anthropic.APIError as exc:
            logger.error("Row %s Claude API error: %s", idx, exc)
            return idx, 0, f"API error: {exc}", str(exc)

    return idx, 0, "Rate limit: max retries exceeded", "rate_limit"


async def rank_rows(df: pd.DataFrame, notifier: Callable) -> pd.DataFrame:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set in .env")

    system_context = Path("prompts/system_context.txt").read_text(errors="replace")
    ranking_prompt_file = os.getenv("RANKING_PROMPT_FILE", "ranking_prompt.txt")
    ranking_prompt = Path(f"prompts/{ranking_prompt_file}").read_text(errors="replace")

    client = anthropic.AsyncAnthropic(api_key=api_key)
    df = df.copy()
    total = len(df)
    await notifier(f"🤖 AI ranking started for {total} rows…")

    scores: dict = {}
    reasons: dict = {}
    errors: dict = {}

    # Process rows concurrently in batches of 3 to respect rate limits
    batch_size = 3
    rows = list(df.iterrows())

    for batch_start in range(0, len(rows), batch_size):
        batch = rows[batch_start: batch_start + batch_size]
        tasks = [
            _score_row(client, system_context, ranking_prompt, row, idx)
            for idx, row in batch
        ]
        results = await asyncio.gather(*tasks)
        for idx, score, reason, error in results:
            scores[idx] = score
            reasons[idx] = reason
            if error:
                errors[idx] = error

        processed = min(batch_start + batch_size, total)
        if processed % _PROGRESS_EVERY == 0 or processed == total:
            await notifier(f"🤖 Ranking progress: {processed}/{total} rows scored…")

        # Small pause between batches to avoid rate-limit bursts
        if batch_start + batch_size < len(rows):
            await asyncio.sleep(3)

    df["Rank Score"] = df.index.map(scores).fillna(0).astype(int)
    df["Rank Reason"] = df.index.map(reasons).fillna("")

    if errors:
        if "Pipeline_Error" not in df.columns:
            df["Pipeline_Error"] = ""
        for idx, err in errors.items():
            df.at[idx, "Pipeline_Error"] = f"Ranking: {err}"

    return df
