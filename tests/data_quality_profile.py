"""Compatibility imports for the former metric module location."""

from cbr_tests.metrics.data_quality import (
    compute_duplicate_row_ratio,
    compute_missing_value_ratio,
)

__all__ = ["compute_duplicate_row_ratio", "compute_missing_value_ratio"]
