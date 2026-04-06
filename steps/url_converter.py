"""Step 2: Convert Sales Navigator URLs to standard LinkedIn profile URLs."""

import re

import pandas as pd

# Column names to probe (case-insensitive)
_URL_COLUMN_CANDIDATES = {
    "profile url", "linkedin url", "profileurl", "linkedinurl",
    "url", "profile_url", "linkedin_url",
}


def _convert_url(url: str) -> str:
    if not isinstance(url, str):
        return url
    url = url.strip()

    # Already a clean linkedin.com/in/ URL
    if re.search(r"linkedin\.com/in/", url) and "sales/" not in url:
        m = re.search(r"linkedin\.com/in/([^/?&#\s]+)", url)
        if m:
            return f"https://www.linkedin.com/in/{m.group(1)}"
        return url

    # Sales Navigator: /sales/lead/... or /sales/people/...
    m = re.search(r"linkedin\.com/sales/(?:lead|people)/([^,\s?&#]+)", url)
    if m:
        # The profile ID sits before the first comma (Sales Nav appends ,NAME_SEARCH,...)
        profile_id = m.group(1).split(",")[0]
        return f"https://www.linkedin.com/in/{profile_id}"

    return url


def convert_urls(df: pd.DataFrame) -> pd.DataFrame:
    """Detect the LinkedIn URL column, convert every URL, and add 'linkedin_url'."""
    df = df.copy()

    url_col = None
    for col in df.columns:
        if col.strip().lower() in _URL_COLUMN_CANDIDATES:
            url_col = col
            break

    # Fallback: find any column that contains linkedin.com values
    if url_col is None:
        for col in df.columns:
            if df[col].astype(str).str.contains("linkedin.com", na=False).any():
                url_col = col
                break

    if url_col is None:
        raise ValueError(
            "No LinkedIn URL column found. "
            "Expected a column named 'Profile URL' or 'LinkedIn URL'."
        )

    df["linkedin_url"] = df[url_col].apply(_convert_url)
    return df
