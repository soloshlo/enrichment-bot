#!/usr/bin/env python3
"""Entry point — Telegram command handlers and bot lifecycle."""

import asyncio
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# Ensure the project root is on sys.path when run from elsewhere
sys.path.insert(0, str(Path(__file__).parent))

from pipeline import Pipeline
from setup_wizard import run_wizard

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ── Global mutable state ──────────────────────────────────────────────────────
_pipeline: Optional[Pipeline] = None
_current_csv: Optional[str] = None
_resume_from: Optional[int] = None  # set by /resume, cleared after run starts

_RESUME_LABELS = {
    3:  "step02_urls_converted.csv  (re-run person enrichment)",
    5:  "step03_persons_enriched.csv  (re-run company enrichment)",
    7:  "step06_merged.csv  (re-run validation onwards)",
    8:  "step07_validated.csv  (re-run ranking onwards)",
    9:  "step08_ranked.csv  (re-run email lookup onwards)",
    11: "step10_emails_found.csv  (re-run email generation only)",
}

_RESUME_DETAILS = {
    3: (
        "Person enrichment (Apify)",
        "step02_urls_converted.csv",
        "This file contains the cleaned LinkedIn profile URLs from step 2. "
        "The bot will send them to Apify to scrape name, title, company, skills, and education.",
    ),
    5: (
        "Company enrichment (Apify)",
        "step03_persons_enriched.csv",
        "This file contains the enriched person records from step 3. "
        "The bot will extract company LinkedIn URLs from it and send them to Apify to scrape company details.",
    ),
    7: (
        "Validation",
        "step06_merged.csv",
        "This file contains the merged person + company data from step 6. "
        "The bot will check for missing fields and duplicate rows.",
    ),
    8: (
        "AI ranking",
        "step07_validated.csv",
        "This file contains the validated data from step 7. "
        "The bot will send each row to Claude to score it 0–100 against your ICP.",
    ),
    9: (
        "Email lookup (Findymail)",
        "step08_ranked.csv",
        "This file contains the ranked leads from step 8. "
        "The bot will ask for approval, then look up work emails for contacts above your score threshold.",
    ),
    11: (
        "Generate outreach emails",
        "step10_emails_found.csv",
        "This file contains the ranked leads with emails from step 10. "
        "The bot will use Claude to write a personalised outreach sequence for each contact.",
    ),
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _update_env_key(key: str, value: str) -> None:
    """Update or add a key=value pair in .env without clobbering other keys."""
    env_path = Path(".env")
    if not env_path.exists():
        env_path.write_text(f"{key}={value}\n")
        return
    lines = env_path.read_text().splitlines()
    updated = False
    new_lines = []
    for line in lines:
        if line.startswith(f"{key}="):
            new_lines.append(f"{key}={value}")
            updated = True
        else:
            new_lines.append(line)
    if not updated:
        new_lines.append(f"{key}={value}")
    env_path.write_text("\n".join(new_lines) + "\n")


# ── Command handlers ──────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "👋 <b>Enrichment Bot</b>\n\n"
        "Automates LinkedIn Sales Navigator enrichment and outreach.\n\n"
        "<b>Commands</b>\n"
        "/upload — Prompt to send your Sales Navigator CSV\n"
        "/run — Start the full 12-step pipeline\n"
        "/status — Show current pipeline progress\n"
        "/setthreshold N — Change the rank score threshold (0–100)\n"
        "/prompts — List all prompt files with a preview\n"
        "/editprompt FILE — Print the full contents of a prompt file\n"
        "/setup — Re-run the setup wizard in the terminal\n"
        "/resume STEP — Resume pipeline from a specific step\n"
        "/setprompt NAME — Switch email prompt (custdev, soply, …)\n"
        "/setranking NAME — Switch ranking prompt (icp, exhibition, …)\n"
        "/steps — Show all pipeline steps and descriptions\n"
        "/help — Show this message",
        parse_mode="HTML",
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await cmd_start(update, context)


async def cmd_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Please send your Sales Navigator CSV file now."
    )


async def cmd_run(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global _pipeline, _current_csv, _resume_from

    if not _current_csv:
        await update.message.reply_text(
            "No CSV loaded yet. Send your CSV file first, or use /upload."
        )
        return

    if _pipeline and _pipeline.is_running():
        await update.message.reply_text(
            "A pipeline is already running. Use /status to check progress."
        )
        return

    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    resume = _resume_from or 1
    _resume_from = None  # clear after use

    _pipeline = Pipeline(
        csv_path=_current_csv,
        bot=context.bot,
        chat_id=chat_id,
        resume_from=resume,
    )

    msg = f"🚀 Starting pipeline from step {resume}…" if resume > 1 else "🚀 Starting pipeline…"
    await update.message.reply_text(msg)
    asyncio.create_task(_pipeline.run())


async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global _resume_from

    valid_steps = list(_RESUME_DETAILS.keys())
    if not context.args or not context.args[0].isdigit() or int(context.args[0]) not in valid_steps:
        await update.message.reply_text(
            "Usage: /resume STEP\n\nUse /steps to see which step numbers are available.",
            parse_mode="HTML",
        )
        return

    step = int(context.args[0])
    _resume_from = step
    name, filename, description = _RESUME_DETAILS[step]
    await update.message.reply_text(
        f"<b>Resume from step {step}: {name}</b>\n\n"
        f"{description}\n\n"
        f"📎 Upload this file:\n<code>{filename}</code>\n\n"
        "Once uploaded, the bot will confirm and you can send /run.",
        parse_mode="HTML",
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _pipeline:
        await update.message.reply_text("No pipeline has been started yet.")
        return
    await update.message.reply_text(_pipeline.get_status())


async def cmd_setthreshold(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Usage: /setthreshold N  (e.g. /setthreshold 8)")
        return
    try:
        n = int(context.args[0])
        if not 0 <= n <= 100:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Please provide an integer between 0 and 100.")
        return

    _update_env_key("RANK_THRESHOLD", str(n))
    load_dotenv(override=True)
    await update.message.reply_text(f"✅ Rank threshold updated to <b>{n}</b>.", parse_mode="HTML")


async def cmd_prompts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    prompts_dir = Path("prompts")
    if not prompts_dir.exists():
        await update.message.reply_text("No prompts/ directory found.")
        return

    parts = ["<b>Prompt files:</b>"]
    for f in sorted(prompts_dir.glob("*.txt")):
        lines = f.read_text(errors="replace").splitlines()
        preview = "\n".join(lines[:2])
        parts.append(f"\n<b>{f.name}</b>\n<code>{preview}</code>")

    await update.message.reply_text("\n".join(parts), parse_mode="HTML")


async def cmd_editprompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text(
            "Usage: /editprompt FILENAME  (e.g. /editprompt ranking_prompt.txt)"
        )
        return

    name = context.args[0]
    path = Path("prompts") / name
    if not path.exists():
        await update.message.reply_text(f"File not found: <code>prompts/{name}</code>", parse_mode="HTML")
        return

    content = path.read_text(errors="replace")
    if len(content) > 3800:
        content = content[:3800] + "\n… [truncated]"

    await update.message.reply_text(
        f"<b>{name}</b>\n\n<code>{content}</code>",
        parse_mode="HTML",
    )


async def cmd_setup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Re-running setup wizard — check your <b>terminal window</b>.",
        parse_mode="HTML",
    )
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, run_wizard)
    load_dotenv(override=True)
    await update.message.reply_text("✅ Setup complete. Configuration reloaded.")


async def cmd_setprompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    prompts_dir = Path("prompts")
    available = sorted(f.stem.replace("email_prompt_", "") for f in prompts_dir.glob("email_prompt_*.txt"))

    if not context.args:
        current = os.getenv("EMAIL_PROMPT_FILE", "email_prompt_custdev.txt").replace("email_prompt_", "").replace(".txt", "")
        lines = "\n".join(f"• <code>{name}</code>" for name in available)
        await update.message.reply_text(
            f"<b>Active email prompt:</b> <code>{current}</code>\n\n"
            f"<b>Available prompts:</b>\n{lines}\n\n"
            "Usage: /setprompt NAME",
            parse_mode="HTML",
        )
        return

    name = context.args[0].lower()
    filename = f"email_prompt_{name}.txt"
    if not (prompts_dir / filename).exists():
        lines = "\n".join(f"• <code>{n}</code>" for n in available)
        await update.message.reply_text(
            f"Prompt <code>{name}</code> not found.\n\nAvailable:\n{lines}",
            parse_mode="HTML",
        )
        return

    _update_env_key("EMAIL_PROMPT_FILE", filename)
    load_dotenv(override=True)
    await update.message.reply_text(
        f"✅ Email prompt set to <b>{name}</b>.",
        parse_mode="HTML",
    )


async def cmd_setranking(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    prompts_dir = Path("prompts")
    available = sorted(f.stem.replace("ranking_prompt_", "") for f in prompts_dir.glob("ranking_prompt_*.txt"))

    if not context.args:
        current = os.getenv("RANKING_PROMPT_FILE", "ranking_prompt.txt").replace("ranking_prompt_", "").replace(".txt", "")
        lines = "\n".join(f"• <code>{name}</code>" for name in available)
        await update.message.reply_text(
            f"<b>Active ranking prompt:</b> <code>{current}</code>\n\n"
            f"<b>Available prompts:</b>\n{lines}\n\n"
            "Usage: /setranking NAME",
            parse_mode="HTML",
        )
        return

    name = context.args[0].lower()
    filename = f"ranking_prompt_{name}.txt"
    if not (prompts_dir / filename).exists():
        lines = "\n".join(f"• <code>{n}</code>" for n in available)
        await update.message.reply_text(
            f"Prompt <code>{name}</code> not found.\n\nAvailable:\n{lines}",
            parse_mode="HTML",
        )
        return

    _update_env_key("RANKING_PROMPT_FILE", filename)
    load_dotenv(override=True)
    await update.message.reply_text(
        f"✅ Ranking prompt set to <b>{name}</b>.",
        parse_mode="HTML",
    )


async def cmd_steps(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = [
        [InlineKeyboardButton("▶️ Resume from step 3: Person enrichment",    callback_data="resume_3")],
        [InlineKeyboardButton("▶️ Resume from step 5: Company enrichment",   callback_data="resume_5")],
        [InlineKeyboardButton("▶️ Resume from step 7: Validation",           callback_data="resume_7")],
        [InlineKeyboardButton("▶️ Resume from step 8: AI ranking",           callback_data="resume_8")],
        [InlineKeyboardButton("▶️ Resume from step 9: Email lookup",         callback_data="resume_9")],
        [InlineKeyboardButton("▶️ Resume from step 11: Generate emails",     callback_data="resume_11")],
    ]
    await update.message.reply_text(
        "<b>Pipeline steps</b>\n\n"
        "1. Load CSV\n"
        "2. Convert URLs\n"
        "3. Person enrichment (Apify)\n"
        "4. Extract company pages\n"
        "5. Company enrichment (Apify)\n"
        "6. Merge data\n"
        "7. Validation\n"
        "8. AI ranking\n"
        "9–10. Email lookup (Findymail)\n"
        "11–12. Generate outreach emails\n\n"
        "Tap a button below to resume from that step:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def handle_prompt_select_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    chosen = query.data.replace("prompt_select_", "")
    label = "Skip" if chosen == "skip" else chosen
    await query.edit_message_text(f"✅ Selected: <b>{label}</b>", parse_mode="HTML")
    if _pipeline and _pipeline.waiting_for_input:
        _pipeline.user_response = chosen
        _pipeline.waiting_for_input = False


async def handle_resume_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global _resume_from
    query = update.callback_query
    await query.answer()

    step = int(query.data.split("_")[1])
    _resume_from = step
    name, filename, description = _RESUME_DETAILS[step]
    await query.edit_message_text(
        f"<b>Resume from step {step}: {name}</b>\n\n"
        f"{description}\n\n"
        f"📎 Upload this file:\n<code>{filename}</code>\n\n"
        "Once uploaded, the bot will confirm and you can send /run.",
        parse_mode="HTML",
    )


# ── Document handler ──────────────────────────────────────────────────────────

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global _current_csv

    doc = update.message.document
    if not doc.file_name.lower().endswith(".csv"):
        await update.message.reply_text("Please send a .csv file.")
        return

    import re
    base = Path(__file__).parent
    (base / "output").mkdir(exist_ok=True)
    safe_stem = re.sub(r"[^\w\-]", "_", Path(doc.file_name).stem)
    save_path = str(base / "output" / f"{safe_stem}.csv")

    tg_file = await context.bot.get_file(doc.file_id)
    await tg_file.download_to_drive(save_path)
    _current_csv = save_path

    try:
        import pandas as pd
        row_count = len(pd.read_csv(save_path, sep=None, engine="python", on_bad_lines="skip"))
    except Exception as exc:
        logger.warning("Could not count CSV rows: %s", exc)
        row_count = "unknown number of"

    if _resume_from and _resume_from in _RESUME_DETAILS:
        name, _, _ = _RESUME_DETAILS[_resume_from]
        await update.message.reply_text(
            f"✅ CSV received — {row_count} rows.\n\n"
            f"Ready to resume from <b>step {_resume_from}: {name}</b>.\n"
            "Send /run to start.",
            parse_mode="HTML",
        )
    else:
        await update.message.reply_text(
            f"✅ CSV received — {row_count} rows.\n"
            "Send /run to start the pipeline."
        )


# ── Text handler (yes/no replies during pipeline pauses) ─────────────────────

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if _pipeline and _pipeline.waiting_for_input:
        _pipeline.user_response = update.message.text.strip().lower()
        _pipeline.waiting_for_input = False


# ── Bot startup ───────────────────────────────────────────────────────────────

def main() -> None:
    if not Path(".env").exists():
        run_wizard()

    load_dotenv()

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        print("ERROR: TELEGRAM_BOT_TOKEN not set. Run the setup wizard first.")
        sys.exit(1)

    base = Path(__file__).parent
    for d in ("output", "logs", "prompts"):
        (base / d).mkdir(exist_ok=True)

    app = (
        Application.builder()
        .token(token)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("upload", cmd_upload))
    app.add_handler(CommandHandler("run", cmd_run))
    app.add_handler(CommandHandler("resume", cmd_resume))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("setthreshold", cmd_setthreshold))
    app.add_handler(CommandHandler("prompts", cmd_prompts))
    app.add_handler(CommandHandler("editprompt", cmd_editprompt))
    app.add_handler(CommandHandler("setup", cmd_setup))
    app.add_handler(CommandHandler("setprompt", cmd_setprompt))
    app.add_handler(CommandHandler("setranking", cmd_setranking))
    app.add_handler(CommandHandler("steps", cmd_steps))
    app.add_handler(CallbackQueryHandler(handle_resume_callback, pattern=r"^resume_\d+$"))
    app.add_handler(CallbackQueryHandler(handle_prompt_select_callback, pattern=r"^prompt_select_"))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    print("Bot is running. Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
