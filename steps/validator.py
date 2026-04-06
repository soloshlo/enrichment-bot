"""Step 7: Data-quality checks on the merged CSV."""

import pandas as pd

REQUIRED_FIELDS = ["full_name", "linkedin_url", "company_name"]


def validate_data(df: pd.DataFrame) -> dict:
    """
    Drop exact duplicate rows, check required fields, and return a summary dict.

    The caller must NOT treat df as modified after this call — pass df.copy() if
    you need to keep the original.  This function operates in-place for efficiency.
    """
    original_len = len(df)
    df.drop_duplicates(inplace=True)
    dupes_removed = original_len - len(df)

    # Only check fields that are actually present
    present = [f for f in REQUIRED_FIELDS if f in df.columns]

    if present:
        empty_mask = df[present].isnull().any(axis=1) | (
            df[present].astype(str).eq("").any(axis=1)
        )
    else:
        empty_mask = pd.Series([False] * len(df), index=df.index)

    missing_count = int(empty_mask.sum())
    complete_count = len(df) - missing_count
    missing_pct = missing_count / len(df) if len(df) > 0 else 0.0

    return {
        "total": len(df),
        "missing": missing_count,
        "complete": complete_count,
        "missing_pct": missing_pct,
        "dupes_removed": dupes_removed,
    }
