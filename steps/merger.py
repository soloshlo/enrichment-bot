"""Step 6: Left-join person rows to company data on company_linkedin_url."""

import re
from typing import List

import pandas as pd

# Legal suffixes to strip before comparing company names
_LEGAL_SUFFIXES = re.compile(
    r"\b(gmbh|inc|ltd|llc|ag|sa|bv|nv|sro|s\.r\.o|oy|ab|as|plc|corp|co|group|holding|holdings|gmbh\s*&\s*co\s*kg)\b",
    re.IGNORECASE,
)


def _normalise_url(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.rstrip("/").str.lower()


def _normalise_name(name: str) -> str:
    """Lowercase, strip legal suffixes and punctuation for fuzzy comparison."""
    if not isinstance(name, str):
        return ""
    name = name.lower()
    name = _LEGAL_SUFFIXES.sub("", name)
    name = re.sub(r"[^a-z0-9\s]", " ", name)
    return re.sub(r"\s+", " ", name).strip()


def _names_match(person_company: str, scraped_company: str) -> bool:
    """
    Return True if the two company names are close enough to be the same company.
    Strategy: check if either normalised name contains the other, or they share
    at least half their significant words.
    """
    a = _normalise_name(person_company)
    b = _normalise_name(scraped_company)

    if not a or not b:
        return True  # can't verify — don't flag

    if a == b:
        return True
    if a in b or b in a:
        return True

    # Word-overlap: at least 50% of the shorter name's words appear in the longer
    words_a = set(a.split())
    words_b = set(b.split())
    shorter = min(words_a, words_b, key=len)
    if not shorter:
        return True
    overlap = len(words_a & words_b) / len(shorter)
    return overlap >= 0.5


def verify_company_matches(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cross-check scraped company_name against person's current_company.
    Adds a 'Company_Match_Warning' column flagging mismatches.
    """
    warnings = []
    for _, row in df.iterrows():
        person_co = str(row.get("current_company", "") or "")
        scraped_co = str(row.get("company_name", "") or "")

        if not scraped_co or scraped_co == "nan":
            warnings.append("")  # no company data — skip
            continue

        if _names_match(person_co, scraped_co):
            warnings.append("")
        else:
            warnings.append(
                f"Mismatch: person says '{person_co}', scraper returned '{scraped_co}'"
            )

    df = df.copy()
    df["Company_Match_Warning"] = warnings
    return df


def merge_data(person_data: List[dict], company_data: List[dict]) -> pd.DataFrame:
    if not person_data:
        raise ValueError("Person data is empty — nothing to merge.")

    person_df = pd.DataFrame(person_data)

    if not company_data:
        return person_df

    company_df = pd.DataFrame(company_data)

    # Normalise join key on both sides
    person_df["_join_key"] = _normalise_url(person_df["company_linkedin_url"])
    company_df["_join_key"] = _normalise_url(company_df["company_linkedin_url"])

    # Drop the raw company_linkedin_url from company_df to avoid duplicate column
    company_df = company_df.drop(columns=["company_linkedin_url"])

    merged = person_df.merge(company_df, on="_join_key", how="left")
    merged = merged.drop(columns=["_join_key"])

    # Cross-check company names and flag mismatches
    merged = verify_company_matches(merged)

    return merged
