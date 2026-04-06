"""Generate emails for consolidated_soply_no_emails.csv using the berman prompt."""

import asyncio
import json
import logging
import os
from pathlib import Path

import anthropic
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_MODEL = "claude-sonnet-4-20250514"
_MAX_TOKENS = 2048
_BATCH_SIZE = 2

EMAIL_COLS = [
    "Email_1_Subject", "Email_1_Body",
    "Email_2_Subject", "Email_2_Body",
    "Email_3_Subject", "Email_3_Body",
    "Email_4_Subject", "Email_4_Body",
]

_SYSTEM_INSTRUCTION = (
    "Return ONLY valid JSON — no markdown fences, no extra text — with this shape:\n"
    '{"email_1":{"subject":"<the one subject line>","body":"..."},'
    '"email_2":{"subject":"Re: <repeat the subject line>","body":"..."},'
    '"email_3":{"subject":"Re: <repeat the subject line>","body":"..."},'
    '"email_4":{"subject":"Re: <repeat the subject line>","body":"..."}}'
)


def _row_to_text(row: pd.Series) -> str:
    skip = set(EMAIL_COLS)
    return "\n".join(
        f"{k}: {v}"
        for k, v in row.items()
        if k not in skip and pd.notna(v) and str(v).strip() != ""
    )


async def _generate_emails(client, system, combined_prompt, row, idx):
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
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            raw = raw.strip()
            parsed = json.loads(raw)
            return idx, parsed, None
        except (json.JSONDecodeError, KeyError) as exc:
            if attempt < 3:
                logger.warning("Row %s JSON error (attempt %s): %s", idx, attempt + 1, exc)
                await asyncio.sleep(2)
            else:
                return idx, None, str(exc)
        except anthropic.RateLimitError:
            wait = 60 * (attempt + 1)
            logger.warning("Row %s rate limited, waiting %ss…", idx, wait)
            await asyncio.sleep(wait)
        except anthropic.APIStatusError as exc:
            if exc.status_code == 529:
                wait = 30 * (attempt + 1)
                logger.warning("Row %s overloaded (attempt %s), waiting %ss…", idx, attempt + 1, wait)
                await asyncio.sleep(wait)
            else:
                return idx, None, str(exc)
        except anthropic.APIError as exc:
            return idx, None, str(exc)
    return idx, None, "Rate limit: max retries exceeded"


async def main():
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set in .env")

    input_path = Path("output/consolidated_soply_sample.csv")
    output_path = Path("output/consolidated_soply_sample.csv")

    system_context = Path("prompts/system_context.txt").read_text(errors="replace")
    prompt = Path("prompts/email_prompt_soply_berman.txt").read_text(errors="replace")

    df = pd.read_csv(input_path, dtype=str).fillna("")

    # Add berman columns next to each existing email column
    berman_cols = []
    for col in EMAIL_COLS:
        berman_col = f"berman_{col}"
        berman_cols.append(berman_col)
        if berman_col not in df.columns:
            # Insert right after the original column
            pos = df.columns.tolist().index(col) + 1
            df.insert(pos, berman_col, "")
        else:
            df[berman_col] = ""

    client = anthropic.AsyncAnthropic(api_key=api_key)
    indices = df.index.tolist()
    logger.info("Generating emails for %d rows…", len(indices))

    for batch_start in range(0, len(indices), _BATCH_SIZE):
        batch = indices[batch_start: batch_start + _BATCH_SIZE]
        tasks = [_generate_emails(client, system_context, prompt, df.loc[idx], idx) for idx in batch]
        results = await asyncio.gather(*tasks)

        for idx, parsed, error in results:
            name = df.at[idx, "full_name"] if "full_name" in df.columns else str(idx)
            if parsed:
                for n in range(1, 5):
                    key = f"email_{n}"
                    if key in parsed:
                        df.at[idx, f"berman_Email_{n}_Subject"] = parsed[key].get("subject", "")
                        df.at[idx, f"berman_Email_{n}_Body"] = parsed[key].get("body", "")
                logger.info("Row %s (%s): emails generated", idx, name)
            if error:
                logger.error("Row %s (%s): %s", idx, name, error)
                if "Pipeline_Error" not in df.columns:
                    df["Pipeline_Error"] = ""
                df.at[idx, "Pipeline_Error"] = f"Email gen: {error}"

        if batch_start + _BATCH_SIZE < len(indices):
            await asyncio.sleep(5)

    df.to_csv(output_path, index=False)
    logger.info("Saved to %s", output_path)


if __name__ == "__main__":
    asyncio.run(main())
