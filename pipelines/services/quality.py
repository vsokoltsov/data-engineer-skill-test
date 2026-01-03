import pandas as pd
from typing import List
from dataclasses import dataclass


@dataclass
class QualityIssue:
    code: str
    message: str
    sample_ids: list[str]


@dataclass
class QualityReport:
    issues_rate: float
    issues: List[QualityIssue]


@dataclass
class BatchQuality:
    threshold: float

    def verify(self, df: pd.DataFrame) -> QualityReport:
        n_rows = df.shape[0]
        issues_rate = 0.0
        issues: List[QualityIssue] = []

        description_mask = (df["description"] == "") | (df["description"].isna())
        huge_amount_mask = df["amount"] > df["amount"].mean() + df["amount"].std()
        small_amount_mask = df["amount"] < df["amount"].mean() - df["amount"].std()
        operation_type_mask = df["operation_type"].isin({"payment", "transfer"})

        missing_desc = df[description_mask]
        if missing_desc.shape[0] > 0:
            issues.append(
                QualityIssue(
                    code="missing_description",
                    message="transaction with missing description",
                    sample_ids=missing_desc["id"].to_list(),
                )
            )
            issues_rate += missing_desc.shape[0] / n_rows

        fishy_amount = df[huge_amount_mask | small_amount_mask]
        if fishy_amount.shape[0] > 0:
            issues.append(
                QualityIssue(
                    code="fishy_amount",
                    message="suspicious amount of transactions",
                    sample_ids=fishy_amount["id"].to_list(),
                )
            )
            issues_rate += fishy_amount.shape[0] / n_rows

        op = df[operation_type_mask]
        if op.shape[0] > 0:
            issues.append(
                QualityIssue(
                    code="wrong_operation_type",
                    message="wrong operation type of transaction",
                    sample_ids=op["id"].to_list(),
                )
            )
            issues_rate += op.shape[0] / n_rows

        return QualityReport(issues_rate=issues_rate, issues=issues)
