# Enrichment Bot

A Telegram bot that automates the full LinkedIn Sales Navigator enrichment and outreach workflow, running entirely on your local machine.

---

## Quick-start

```bash
cd enrichment-bot
pip install -r requirements.txt
python bot.py          # setup wizard runs automatically on first launch
```

1. Follow the wizard prompts in the terminal — it collects every API key and writes `.env` plus all prompt files.
2. Open Telegram, find your bot, and send `/start`.
3. Send your Sales Navigator CSV export via the Telegram chat.
4. Send `/run` to kick off the pipeline.

---

## Pipeline steps

| # | Step | Description |
|---|------|-------------|
| 1 | **Load CSV** | Receives the CSV you sent via Telegram and loads it into memory. |
| 2 | **Convert URLs** | Detects the LinkedIn URL column and converts Sales Navigator `/sales/lead/` and `/sales/people/` URLs to standard `linkedin.com/in/` URLs. |
| 3 | **Person enrichment** | Runs your Apify actor to scrape full name, headline, location, title, company, education, skills, connections count, and company LinkedIn URL for each profile. |
| 4 | **Extract company pages** | De-duplicates the `companyLinkedInUrl` values from step 3 to build the list of companies to enrich. |
| 5 | **Company enrichment** | Runs your second Apify actor to scrape company name, industry, headcount range, founded year, description, website, specialties, and HQ location. |
| 6 | **Merge** | Left-joins person rows to company data on `company_linkedin_url` and saves `output/enriched_TIMESTAMP.csv`. |
| 7 | **Validation** | Removes duplicate rows; checks that `full_name`, `linkedin_url`, and `company_name` are non-empty. Pauses and asks via Telegram if >50 % of rows have missing data. |
| 8 | **AI ranking** | Calls Claude (`claude-sonnet-4-20250514`) to score each row 1–10 against your ICP. Adds `Rank Score` and `Rank Reason` columns. Saves `output/ranked_TIMESTAMP.csv`. |
| 9–10 | **Email lookup** | Calls the Findymail API (max 5 req/s) for every row that meets the rank threshold. Adds an `Email` column (`Not found` when no match). Saves `output/enriched_with_emails_TIMESTAMP.csv`. |
| 11–12 | **Generate & insert emails** | Calls Claude for each row with a valid email to write four personalised cold emails (problem-aware, social proof, direct ask, personalised insight). Adds 8 columns (`Email_1_Subject`, `Email_1_Body`, …) and saves `output/final_TIMESTAMP.csv`. |

---

## Telegram commands

| Command | Description |
|---------|-------------|
| `/start` | Welcome message and command list |
| `/upload` | Prompt to send your CSV file |
| `/run` | Start the full pipeline |
| `/status` | Show current step and progress |
| `/setthreshold N` | Change the rank score threshold (1–10) |
| `/prompts` | List all prompt files with a 2-line preview |
| `/editprompt FILE` | Print the full contents of a prompt file |
| `/setup` | Re-run the setup wizard in the terminal |
| `/help` | Show command list |

---

## Configuration

All configuration lives in `.env` (created by the setup wizard):

| Key | Description |
|-----|-------------|
| `TELEGRAM_BOT_TOKEN` | From @BotFather |
| `TELEGRAM_CHAT_ID` | Your personal chat ID (find via @userinfobot) |
| `APIFY_API_TOKEN` | Apify API token |
| `APIFY_ACTOR_PERSON_ID` | Actor ID for LinkedIn profile scraper |
| `APIFY_ACTOR_COMPANY_ID` | Actor ID for LinkedIn company scraper |
| `FINDYMAIL_API_KEY` | Findymail API key |
| `ANTHROPIC_API_KEY` | Anthropic API key (Claude) |
| `RANK_THRESHOLD` | Minimum score to qualify for outreach (default `7`) |

---

## Prompt files

| File | Purpose |
|------|---------|
| `prompts/system_context.txt` | Injected as the system prompt in every Claude call |
| `prompts/ranking_prompt.txt` | ICP scoring instructions for step 8 |
| `prompts/outreach_email_1.txt` | Email angle: problem-aware |
| `prompts/outreach_email_2.txt` | Email angle: social proof |
| `prompts/outreach_email_3.txt` | Email angle: direct ask |
| `prompts/outreach_email_4.txt` | Email angle: personalised insight |

Edit any file directly or use `/editprompt` to view it in Telegram.

---

## Credit monitoring

The bot automatically checks and warns you when:
- **Apify** compute units fall below 1 000 (checked before and after each Apify step)
- **Findymail** credits fall below 50 (checked before and after the email lookup step)

---

## Output files

All files are saved to `output/` with a timestamp suffix:

```
output/
  input_TIMESTAMP.csv               ← your original upload
  enriched_TIMESTAMP.csv            ← after merge (step 6)
  ranked_TIMESTAMP.csv              ← after AI ranking (step 8)
  enriched_with_emails_TIMESTAMP.csv ← after email lookup (steps 9–10)
  final_TIMESTAMP.csv               ← complete, ready for outreach (step 12)
```

---

## Error handling

- Every step is wrapped in `try/except`. Failures send a Telegram alert with the step name and error detail.
- Row-level errors skip the failed row, add a `Pipeline_Error` note to the CSV, and continue.
- Apify actor timeouts (>30 min) alert you and halt the pipeline.
- Claude JSON parse failures retry once, then skip the row and flag it.
- All events are logged to `logs/pipeline_TIMESTAMP.log`.
