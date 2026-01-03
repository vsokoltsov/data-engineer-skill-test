import pandas as pd
from typing import Dict, Any


class TransactionValidator:

    @classmethod
    def validate_rows(cls, rows: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        df = rows.copy()

        reasons: pd.Series[Any] = pd.Series(
            [[] for _ in range(len(df))], index=df.index, dtype=object
        )

        # Check for null values
        amount_null = df["amount"].isna()
        side_null = df["side"].isna()
        side_invalid = ~df["side"].isin({"debit", "credit"})

        # Check amount-side consistency:
        # - debit transactions must have amount < 0
        # - credit transactions must have amount > 0
        # - amount == 0 is invalid for both
        amount_side_mismatch = ((df["side"] == "debit") & (df["amount"] >= 0)) | (
            (df["side"] == "credit") & (df["amount"] <= 0)
        )

        reasons.loc[amount_null] = reasons.loc[amount_null].apply(
            lambda r: r + ["amount is null"]
        )
        reasons.loc[side_null] = reasons.loc[side_null].apply(
            lambda r: r + ["side is null"]
        )
        reasons.loc[side_invalid] = reasons.loc[side_invalid].apply(
            lambda r: r + ["side is not valid"]
        )
        reasons.loc[amount_side_mismatch] = reasons.loc[amount_side_mismatch].apply(
            lambda r: r
            + [
                "amount and side mismatch: debit must have amount < 0, credit must have amount > 0"
            ]
        )

        df["validation_error"] = reasons.map(lambda item: ";".join(item))

        invalid_mask = df["validation_error"].str.len() > 0

        return {
            "valid": df.loc[~invalid_mask].drop(columns=["validation_error"]),
            "dlqs": df.loc[invalid_mask],
        }
