# Core data quality metrics

This page documents every dispatcher metric in the **Core data quality** category. Return to the [complete metric index](../metric_reference.md).

## `column_quality_profile`

Profiles whether requested columns exist, contain values, coerce to numeric data, vary, and are usable for analysis.

- **Implementation:** `cbr_tests/metrics/column_quality.py`
- **Supplied-plan usage:** `deepsecure_plan`, `fortisedos_plan`, `secure5g_plan`
- **Inputs:** `candidate_fields` list.
- **Primary output:** Per-field counts/ratios and a summary `quality_score` equal to usable fields divided by requested fields.
- **Interpretation:** Higher `quality_score` is better; inspect field reasons for missing, empty, nonnumeric, insufficient, or constant columns.
- **Current caveat:** This is a structural numeric-usability score, not a complete dataset-quality judgment.

## `duplicate_row_ratio`

Counts rows duplicated after the first occurrence.

- **Implementation:** `cbr_tests/metrics/data_quality.py`
- **Supplied-plan usage:** `deepsecure_plan`, `deepslice_plan`, `fortisedos_plan`, `secure5g_plan`
- **Inputs:** Optional `subset_fields`; absent means all columns.
- **Primary output:** Duplicate row/group counts and duplicate rows divided by total rows.
- **Interpretation:** Lower is usually better, but legitimate repeated observations may be expected.
- **Current caveat:** A missing/empty subset produces zero duplicate rows rather than comparing all columns.

## `missing_value_ratio`

Measures null cells over selected fields.

- **Implementation:** `cbr_tests/metrics/data_quality.py`
- **Supplied-plan usage:** `deepsecure_plan`, `deepslice_plan`, `fortisedos_plan`, `secure5g_plan`
- **Inputs:** Optional `candidate_fields`; absent means all dataframe columns.
- **Primary output:** Per-field missing counts and overall missing cells divided by total selected cells.
- **Interpretation:** Lower is better. A value of 0 means no pandas-null values in selected cells.
- **Current caveat:** Empty strings are not automatically null unless the loader/parser interprets them as such.
