"""Interactive terminal setup wizard — runs on first launch or via /setup."""

from pathlib import Path


def _ask(prompt: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    while True:
        value = input(f"{prompt}{suffix}: ").strip()
        if value:
            return value
        if default:
            return default
        print("  (required — please enter a value)")


def _multiline_input(hint: str) -> str:
    """Read lines until the user submits an empty line."""
    print(hint)
    print("  (press Enter on an empty line to finish)")
    lines = []
    while True:
        line = input()
        if line == "":
            if lines:
                break
        else:
            lines.append(line)
    return "\n".join(lines)


def run_wizard() -> None:
    print()
    print("=" * 60)
    print("  ENRICHMENT BOT — SETUP WIZARD")
    print("=" * 60)
    print("Answer each prompt. Press Enter to accept the default.\n")

    tg_token = _ask("1.  Telegram Bot Token (from @BotFather)")
    tg_chat_id = _ask("2.  Telegram Chat ID (send /start to @userinfobot to find it)")
    apify_token = _ask("3.  Apify API Token")
    apify_person_id = _ask("4.  Apify Actor ID for LinkedIn person enrichment")
    apify_company_id = _ask("5.  Apify Actor ID for LinkedIn company enrichment")
    findymail_key = _ask("6.  Findymail API Key")
    anthropic_key = _ask("7.  Anthropic API Key (for ranking and email generation)")
    rank_threshold = _ask("8.  Minimum rank score for outreach? (1–10)", "7")

    company_context = _multiline_input(
        "\n9.  Briefly describe your company and product\n"
        "    (used as system prompt in every Claude API call):"
    )

    icp_criteria = _multiline_input(
        "\n10. Describe your ideal customer profile and scoring criteria\n"
        "    (e.g. SaaS companies 50–500 employees, VP or above, using Salesforce):"
    )

    email_style = _multiline_input(
        "\n11. Describe your outreach tone and value proposition\n"
        "    (e.g. direct and concise, focus on ROI, no buzzwords):"
    )

    # ── Write .env ──────────────────────────────────────────────────────────
    env_path = Path(".env")
    env_path.write_text(
        f"TELEGRAM_BOT_TOKEN={tg_token}\n"
        f"TELEGRAM_CHAT_ID={tg_chat_id}\n"
        f"APIFY_API_TOKEN={apify_token}\n"
        f"APIFY_ACTOR_PERSON_ID={apify_person_id}\n"
        f"APIFY_ACTOR_COMPANY_ID={apify_company_id}\n"
        f"FINDYMAIL_API_KEY={findymail_key}\n"
        f"ANTHROPIC_API_KEY={anthropic_key}\n"
        f"RANK_THRESHOLD={rank_threshold}\n"
    )
    print(f"\n✅ Written: {env_path}")

    # ── Write prompt files ───────────────────────────────────────────────────
    Path("prompts").mkdir(exist_ok=True)

    system_path = Path("prompts/system_context.txt")
    system_path.write_text(company_context)
    print(f"✅ Written: {system_path}")

    ranking_path = Path("prompts/ranking_prompt.txt")
    ranking_path.write_text(
        f"{icp_criteria}\n\n"
        "Score this lead from 1 to 10 based on fit with the ideal customer profile above.\n"
        'Return ONLY valid JSON: {"score": <number>, "reason": "<one-sentence explanation>"}'
    )
    print(f"✅ Written: {ranking_path}")

    angles = [
        (
            "outreach_email_1.txt",
            "Angle: problem-aware — open with a pain point they likely have",
            (
                f"{email_style}\n\n"
                "Write a cold email that leads with the recipient's likely pain point, "
                "then offers a specific solution. Keep it under 100 words."
            ),
        ),
        (
            "outreach_email_2.txt",
            "Angle: social proof — reference a relevant customer win or case study",
            (
                f"{email_style}\n\n"
                "Write a cold email that opens with a brief result you achieved for a "
                "similar company, then ties it to the recipient's situation."
            ),
        ),
        (
            "outreach_email_3.txt",
            "Angle: direct ask — short, confident, ask for a specific next step",
            (
                f"{email_style}\n\n"
                "Write a very short cold email (under 60 words) that gets straight to "
                "the point and ends with a clear, low-friction call to action."
            ),
        ),
        (
            "outreach_email_4.txt",
            "Angle: personalised insight — reference something specific from their "
            "LinkedIn profile or company page to show genuine research",
            (
                f"{email_style}\n\n"
                "Write a cold email that opens with a specific observation about the "
                "recipient's background or company, making it feel individually crafted."
            ),
        ),
    ]

    for filename, header, body in angles:
        path = Path("prompts") / filename
        path.write_text(f"# {header}\n\n{body}")
        print(f"✅ Written: {path}")

    print()
    print("=" * 60)
    print("  Setup complete — launching bot…")
    print("=" * 60)
    print()
