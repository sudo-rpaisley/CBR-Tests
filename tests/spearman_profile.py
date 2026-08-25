"""Compatibility imports for the former metric module location."""

from cbr_tests.metrics.spearman import (
    compute_spearman_profile,
    validate_spearman_candidate_fields,
)

__all__ = ["compute_spearman_profile", "validate_spearman_candidate_fields"]
