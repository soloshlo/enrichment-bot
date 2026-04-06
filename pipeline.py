"""Pipeline orchestrator — runs all 12 steps sequentially and sends Telegram updates."""

import asyncio
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup

from steps.url_converter import convert_urls
from steps.apify_person import enrich_persons
from steps.company_extractor import extract_companies
from steps.apify_company import enrich_companies
from steps.merger import merge_data
from steps.validator import validate_data
from steps.ranker import rank_rows
from steps.email_finder import find_emails
from steps.email_writer import write_emails as write_emails_claude
from steps.email_writer_gemini import write_emails as write_emails_gemini

logger = logging.getLogger(__name__)


def _setup_file_logger(timestamp: str) -> None:
    Path("logs").mkdir(exist_ok=True)
    fh = logging.FileHandler(f"logs/pipeline_{timestamp}.log")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logging.getLogger().addHandler(fh)


class Pipeline:
    def __init__(self, csv_path: str, bot: Bot, chat_id: str, resume_from: int = 1) -> None:
        self.csv_path = csv_path
        self.bot = bot
        self.chat_id = chat_id
        self.current_step: int = 0
        self.total_steps: int = 12
        self._running: bool = False
        self.waiting_for_input: bool = False
        self.user_response: Optional[str] = None
        self.resume_from: int = resume_from
        self.timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.csv_stem: str = Path(csv_path).stem  # e.g. "leads_march" from "leads_march.csv"
        # One folder per run: output/leads_march_20260317_143000/
        run_dir = Path(__file__).parent / "output" / f"{self.csv_stem}_{self.timestamp}"
        run_dir.mkdir(parents=True, exist_ok=True)
        self.run_dir: Path = run_dir

        # Move the original upload into the run folder
        import shutil
        dest = run_dir / "step01_input.csv"
        shutil.move(csv_path, dest)
        self.csv_path = str(dest)

        _setup_file_logger(self.timestamp)

    # ── Public helpers ────────────────────────────────────────────────────────

    def is_running(self) -> bool:
        return self._running

    def get_status(self) -> str:
        if not self._running and self.current_step == 0:
            return "Pipeline has not started yet."
        if self._running:
            return (
                f"▶️ Running step {self.current_step}/{self.total_steps} — "
                f"{self._step_name(self.current_step)}"
            )
        return f"✅ Pipeline finished (last step: {self.current_step}/{self.total_steps})"

    # ── Internal helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _step_name(n: int) -> str:
        names = {
            1: "Load CSV",
            2: "Convert URLs",
            3: "Person enrichment (Apify)",
            4: "Extract company pages",
            5: "Company enrichment (Apify)",
            6: "Merge data",
            7: "Validation",
            8: "AI ranking",
            9: "Email lookup (Findymail)",
            10: "Enrich with emails",
            11: "Generate outreach emails",
            12: "Save final CSV",
        }
        return names.get(n, f"Step {n}")

    async def _notify(self, text: str) -> None:
        logger.info(text.replace("<b>", "").replace("</b>", "").replace("<code>", "").replace("</code>", ""))
        try:
            await self.bot.send_message(
                chat_id=self.chat_id, text=text, parse_mode="HTML"
            )
        except Exception as exc:
            logger.error("Failed to send Telegram message: %s", exc)

    async def _wait_for_user(self, prompt: str) -> str:
        """Send a prompt to Telegram and block until the user replies."""
        await self._notify(prompt)
        self.waiting_for_input = True
        self.user_response = None
        while self.waiting_for_input:
            await asyncio.sleep(1)
        return self.user_response or ""

    async def _wait_for_button(self, prompt: str, buttons: list[tuple[str, str]]) -> str:
        """Send a message with inline buttons and block until the user taps one.
        buttons: list of (label, callback_data) tuples.
        Returns the callback_data of the tapped button.
        """
        keyboard = [[InlineKeyboardButton(label, callback_data=data)] for label, data in buttons]
        self.waiting_for_input = True
        self.user_response = None
        await self.bot.send_message(
            chat_id=self.chat_id,
            text=prompt,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        while self.waiting_for_input:
            await asyncio.sleep(1)
        return self.user_response or ""

    def _out(self, name: str) -> str:
        return str(self.run_dir / f"{name}.csv")

    # ── Main run ──────────────────────────────────────────────────────────────

    def _load_csv(self) -> pd.DataFrame:
        return pd.read_csv(self.csv_path, sep=None, engine="python", on_bad_lines="skip")

    def _load_csv_from(self, path: str) -> pd.DataFrame:
        return pd.read_csv(path, sep=None, engine="python", on_bad_lines="skip")

    async def run(self) -> None:
        self._running = True
        start = self.resume_from

        # ── Pre-load data when resuming from a mid-pipeline step ─────────────
        df: Optional[pd.DataFrame] = None
        person_data: list = []
        company_data: list = []
        company_urls: list = []
        merged_df: Optional[pd.DataFrame] = None
        ranked_df: Optional[pd.DataFrame] = None
        enriched_email_df: Optional[pd.DataFrame] = None

        try:
            if start == 1:
                await self._notify("🚀 <b>Pipeline started</b>")
            else:
                preloaded = self._load_csv()
                await self._notify(
                    f"▶️ <b>Resuming from step {start}</b> — "
                    f"{len(preloaded)} rows loaded from uploaded CSV."
                )
                if start == 3:
                    df = preloaded
                elif start == 5:
                    person_data = preloaded.to_dict("records")
                elif start in (7, 8):
                    merged_df = preloaded
                elif start == 9:
                    ranked_df = preloaded
                elif start == 11:
                    enriched_email_df = preloaded

            # ── Step 1: Load CSV ──────────────────────────────────────────────
            if start <= 1:
                self.current_step = 1
                try:
                    df = self._load_csv()
                    await self._notify(
                        f"✅ <b>Step 1</b> — CSV loaded: {len(df)} rows."
                    )
                except Exception as exc:
                    await self._notify(f"❌ <b>Step 1 failed</b>: {exc}")
                    return

            # ── Step 2: Convert URLs ──────────────────────────────────────────
            if start <= 2:
                self.current_step = 2
                try:
                    df = convert_urls(df)
                    converted = df["linkedin_url"].notna().sum()
                    p = self._out("step02_urls_converted")
                    df.to_csv(p, index=False)
                    await self._notify(
                        f"✅ <b>Step 2</b> — URLs converted for {converted} profiles.\n"
                        f"Saved to <code>{p}</code>"
                    )
                except Exception as exc:
                    await self._notify(f"❌ <b>Step 2 failed</b>: {exc}")
                    return

            # ── Step 3: Person enrichment ─────────────────────────────────────
            if start <= 3:
                self.current_step = 3
                try:
                    profile_urls = df["linkedin_url"].dropna().tolist()
                    person_data = await enrich_persons(profile_urls, self._notify)
                    p = self._out("step03_persons_enriched")
                    pd.DataFrame(person_data).to_csv(p, index=False)
                    await self._notify(
                        f"✅ <b>Step 3</b> — Person enrichment complete — "
                        f"{len(person_data)} profiles enriched.\n"
                        f"Saved to <code>{p}</code>"
                    )
                except Exception as exc:
                    await self._notify(f"❌ <b>Step 3 failed</b>: {exc}")
                    return

            # ── Step 4: Extract company pages ─────────────────────────────────
            # Runs when start ≤ 5 so that resuming at step 5 still gets company_urls.
            if start <= 5:
                self.current_step = 4
                try:
                    company_urls = extract_companies(person_data)
                    await self._notify(
                        f"✅ <b>Step 4</b> — Found {len(company_urls)} unique company pages to enrich."
                    )
                except Exception as exc:
                    await self._notify(f"❌ <b>Step 4 failed</b>: {exc}")
                    return

            # ── Step 5: Company enrichment ────────────────────────────────────
            if start <= 5:
                self.current_step = 5
                try:
                    company_data = await enrich_companies(company_urls, self._notify)
                    p = self._out("step05_companies_enriched")
                    pd.DataFrame(company_data).to_csv(p, index=False)
                    await self._notify(
                        f"✅ <b>Step 5</b> — Company enrichment complete — "
                        f"{len(company_data)} companies enriched.\n"
                        f"Saved to <code>{p}</code>"
                    )
                except Exception as exc:
                    await self._notify(f"❌ <b>Step 5 failed</b>: {exc}")
                    return

            # ── Step 6: Merge ─────────────────────────────────────────────────
            if start <= 6:
                self.current_step = 6
                try:
                    merged_df = merge_data(person_data, company_data)
                    p = self._out("step06_merged")
                    merged_df.to_csv(p, index=False)
                    mismatches = int((merged_df.get("Company_Match_Warning", pd.Series(dtype=str)) != "").sum())
                    mismatch_msg = (
                        f"\n⚠️ {mismatches} company name mismatches — check <b>Company_Match_Warning</b> column."
                        if mismatches else ""
                    )
                    await self._notify(
                        f"✅ <b>Step 6</b> — Merge complete — "
                        f"{len(merged_df)} rows, {len(merged_df.columns)} columns."
                        f"{mismatch_msg}\n"
                        f"Saved to <code>{p}</code>"
                    )
                except Exception as exc:
                    await self._notify(f"❌ <b>Step 6 failed</b>: {exc}")
                    return

            # ── Step 7: Validation ────────────────────────────────────────────
            if start <= 7:
                self.current_step = 7
                try:
                    vr = validate_data(merged_df)
                    dupes_msg = (
                        f" ({vr['dupes_removed']} duplicate rows removed.)" if vr["dupes_removed"] else ""
                    )
                    p = self._out("step07_validated")
                    merged_df.to_csv(p, index=False)
                    await self._notify(
                        f"✅ <b>Step 7</b> — Validation{dupes_msg}\n"
                        f"{vr['missing']} rows have missing required fields. "
                        f"{vr['complete']} rows are complete.\n"
                        f"Saved to <code>{p}</code>"
                    )
                    if vr["missing_pct"] > 0.5:
                        reply = await self._wait_for_user(
                            "⚠️ Over 50% of rows have missing data.\n"
                            "Reply <b>yes</b> to continue or <b>no</b> to abort."
                        )
                        if reply.lower() != "yes":
                            await self._notify("🛑 Pipeline aborted by user.")
                            return
                except Exception as exc:
                    await self._notify(f"❌ <b>Step 7 failed</b>: {exc}")
                    return

            # ── Step 8: AI Ranking ────────────────────────────────────────────
            if start <= 8:
                self.current_step = 8
                try:
                    ranking_prompts_dir = Path(__file__).parent / "prompts"
                    available_ranking = sorted(
                        p.stem.replace("ranking_prompt_", "")
                        for p in ranking_prompts_dir.glob("ranking_prompt_*.txt")
                    )
                    ranking_buttons = [(name, f"prompt_select_{name}") for name in available_ranking]
                    chosen_ranking = await self._wait_for_button(
                        "🎯 <b>Choose a ranking prompt for step 8:</b>",
                        ranking_buttons,
                    )
                    os.environ["RANKING_PROMPT_FILE"] = f"ranking_prompt_{chosen_ranking}.txt"
                    await self._notify(f"✅ Using ranking prompt: <b>{chosen_ranking}</b>")
                    from steps.validator import REQUIRED_FIELDS
                    present = [f for f in REQUIRED_FIELDS if f in merged_df.columns]
                    incomplete_mask = (
                        merged_df[present].isnull().any(axis=1)
                        | merged_df[present].astype(str).eq("").any(axis=1)
                    ) if present else pd.Series([False] * len(merged_df), index=merged_df.index)
                    complete_df = merged_df[~incomplete_mask].copy()
                    incomplete_df = merged_df[incomplete_mask].copy()
                    incomplete_df["Rank Score"] = 0
                    incomplete_df["Rank Reason"] = "Incomplete data"
                    ranked_complete = await rank_rows(complete_df, self._notify)
                    ranked_df = pd.concat([ranked_complete, incomplete_df]).sort_index()
                    threshold = int(os.getenv("RANK_THRESHOLD", "7"))
                    p = self._out("step08_ranked")
                    ranked_df.to_csv(p, index=False)
                    qualified_count = int((ranked_df["Rank Score"] >= threshold).sum())
                    await self._notify(
                        f"✅ <b>Step 8</b> — Ranking complete — "
                        f"{qualified_count} accounts scored ≥ {threshold}.\n"
                        f"Saved to <code>{p}</code>"
                    )
                    await self._wait_for_user(
                        f"⏸️ <b>Review the ranked file before continuing.</b>\n"
                        f"Open and edit <code>{p}</code> on your laptop if needed.\n\n"
                        "Reply <b>yes</b> when ready to continue."
                    )
                    ranked_df = self._load_csv_from(p)
                    await self._notify(
                        f"✅ Ranked file reloaded — {len(ranked_df)} rows."
                    )
                except Exception as exc:
                    await self._notify(f"❌ <b>Step 8 failed</b>: {exc}")
                    return

            # ── Steps 9–10: Find emails ───────────────────────────────────────
            if start <= 9:
                self.current_step = 9
                try:
                    default_threshold = int(os.getenv("RANK_THRESHOLD", "7"))
                    while True:
                        raw = await self._wait_for_user(
                            f"⏸️ <b>Email lookup threshold</b>\n"
                            f"Enter the minimum rank score for email lookup (0–100).\n"
                            f"Current default: <b>{default_threshold}</b>"
                        )
                        if raw.strip().isdigit() and 0 <= int(raw.strip()) <= 100:
                            threshold = int(raw.strip())
                            break
                        await self._notify("⚠️ Please enter a whole number between 0 and 100.")
                    qualified_df = ranked_df[ranked_df["Rank Score"] >= threshold].copy()

                    reply = await self._wait_for_user(
                        f"✅ Threshold set to <b>{threshold}</b> — "
                        f"{len(qualified_df)} contacts qualify.\n\n"
                        "Reply <b>yes</b> to start email lookup or <b>no</b> to skip."
                    )
                    if reply.lower() != "yes":
                        await self._notify("⏭️ Email lookup skipped. Continuing without emails.")
                        enriched_email_df = ranked_df.copy()
                        enriched_email_df["Email"] = "Not found"
                    else:
                        enriched_email_df = await find_emails(ranked_df, qualified_df, self._notify)
                    emails_found = int((enriched_email_df["Email"] != "Not found").sum())
                    p = self._out("step10_emails_found")
                    enriched_email_df.to_csv(p, index=False)
                    await self._notify(
                        f"✅ <b>Steps 9–10</b> — Email search complete — "
                        f"{emails_found} emails found out of {len(qualified_df)} attempts.\n"
                        f"Saved to <code>{p}</code>"
                    )
                except Exception as exc:
                    await self._notify(f"❌ <b>Steps 9–10 failed</b>: {exc}")
                    return

            # ── Steps 11–12: Generate and insert outreach emails ──────────────
            self.current_step = 11
            try:
                with_email = int((enriched_email_df["Email"] != "Not found").sum())
                prompts_dir = Path(__file__).parent / "prompts"
                available = sorted(p.stem.replace("email_prompt_", "") for p in prompts_dir.glob("email_prompt_*.txt"))
                buttons = [(name, f"prompt_select_{name}") for name in available]
                buttons.append(("⏭️ Skip email generation", "prompt_select_skip"))
                chosen = await self._wait_for_button(
                    f"⏸️ <b>Ready to generate outreach emails.</b>\n"
                    f"{with_email} contacts have emails. Choose a prompt:",
                    buttons,
                )
                if chosen == "skip":
                    await self._notify("⏭️ Email generation skipped. Pipeline complete.")
                    return
                os.environ["EMAIL_PROMPT_FILE"] = f"email_prompt_{chosen}.txt"
                await self._notify(f"✅ Using prompt: <b>{chosen}</b>")

                model_choice = await self._wait_for_button(
                    "🤖 <b>Which AI model should write the emails?</b>",
                    [
                        ("Claude (Anthropic)", "prompt_select_model_claude"),
                        ("Gemini (Google)", "prompt_select_model_gemini"),
                    ],
                )
                final_path = self._out(f"step12_final_{chosen}")
                if model_choice == "model_gemini":
                    await self._notify("✅ Using <b>Gemini</b> for email generation.")
                    final_df = await write_emails_gemini(enriched_email_df, self._notify, save_path=final_path)
                else:
                    await self._notify("✅ Using <b>Claude</b> for email generation.")
                    final_df = await write_emails_claude(enriched_email_df, self._notify, save_path=final_path)
                final_df.to_csv(final_path, index=False)
                ready = int(
                    (final_df["Subject"].notna() & (final_df["Subject"] != "")).sum()
                )
                await self._notify(
                    f"✅ <b>Pipeline complete!</b>\n"
                    f"Final CSV saved to <code>{final_path}</code>\n"
                    f"{ready} rows ready for outreach."
                )
                self.current_step = 12
            except Exception as exc:
                await self._notify(f"❌ <b>Steps 11–12 failed</b>: {exc}")
                return

        except Exception as exc:
            await self._notify(f"💥 <b>Unexpected pipeline error</b>: {exc}")
            logger.exception("Unexpected pipeline error")
        finally:
            self._running = False
