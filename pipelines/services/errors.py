from dataclasses import dataclass


@dataclass
class MLAPIServiceError(Exception):
    orig: Exception
