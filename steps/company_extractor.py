"""Step 4: Extract unique company LinkedIn URLs from person enrichment results."""

from typing import List


def extract_companies(person_data: List[dict]) -> List[str]:
    """Return de-duplicated, non-empty company LinkedIn URLs."""
    seen: set = set()
    urls: List[str] = []
    for person in person_data:
        raw = person.get("company_linkedin_url", "")
        if not isinstance(raw, str):
            continue
        url = raw.strip()
        if url and url not in seen:
            seen.add(url)
            urls.append(url)
    return urls
