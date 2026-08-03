# Temporal integrity and drift metrics

This page documents every dispatcher metric in the **Temporal integrity and drift** category. Return to the [complete metric index](../metric_reference.md).

## `burstiness_coefficient_deviation`

Compares burstiness coefficients of first- and second-half inter-arrival gaps.

- **Implementation:** `cbr_tests/metrics/temporal.py`
- **Supplied-plan usage:** `deepsecure_plan`
- **Inputs:** `timestamp_field`.
- **Primary output:** Both burstiness values and their absolute deviation.
- **Interpretation:** Lower deviation means more stable burstiness.
- **Current caveat:** No explicit minimum-sample threshold; short/constant samples can be uninformative.

## `diurnal_pattern_similarity_score`

Computes cosine similarity between 24-hour count vectors from timestamp halves.

- **Implementation:** `cbr_tests/metrics/temporal.py`
- **Supplied-plan usage:** `deepsecure_plan`
- **Inputs:** `timestamp_field`.
- **Primary output:** Sample counts and similarity score.
- **Interpretation:** Closer to 1 means a more similar hourly shape.
- **Current caveat:** Both all-zero/empty vectors produce 0; raw counts make the score sensitive to sparse hours.

## `hourly_activity_distribution_divergence`

Compares 24-hour activity distributions between timestamp halves using total variation distance.

- **Implementation:** `cbr_tests/metrics/temporal.py`
- **Supplied-plan usage:** `deepsecure_plan`
- **Inputs:** `timestamp_field`.
- **Primary output:** Sample counts and divergence in [0,1].
- **Interpretation:** 0 means identical hourly proportions; 1 means disjoint hourly activity.
- **Current caveat:** Internal half comparison and sensitive to the capture interval and timezone interpretation.

## `inter_arrival_time_distribution_divergence`

KS-compares inter-arrival times from the first and second halves of sorted timestamps.

- **Implementation:** `cbr_tests/metrics/temporal.py`
- **Supplied-plan usage:** `deepsecure_plan`
- **Inputs:** `timestamp_field`; `minimum_sample_size` default 2.
- **Primary output:** Gap/sample counts, runnable flag, and KS divergence.
- **Interpretation:** Lower indicates more stable inter-arrival behavior.
- **Current caveat:** Internal two-half comparison, not external fidelity. Sorting removes original row order before gap calculation.

## `non_negative_duration_ratio`

Checks numeric durations are nonnegative, or derives duration from start/end timestamps.

- **Implementation:** `cbr_tests/metrics/temporal.py`
- **Supplied-plan usage:** `deepsecure_plan`
- **Inputs:** Optional `duration_field`; otherwise start/end timestamp fields.
- **Primary output:** Valid/negative counts and nonnegative ratio.
- **Interpretation:** Higher is better.
- **Current caveat:** Only nonnegative sign is assessed; implausibly large positive durations are not rejected.

## `periodicity_preservation_score`

Compares autocorrelation at configured lags between timestamp halves.

- **Implementation:** `cbr_tests/metrics/temporal.py`
- **Supplied-plan usage:** `deepsecure_plan`
- **Inputs:** `timestamp_field`; `lags` default `[1,24]`.
- **Primary output:** Per-lag autocorrelations/deviations and 1 minus mean capped deviation.
- **Interpretation:** Higher means more similar autocorrelation structure.
- **Current caveat:** Hourly count vectors always have length 24, so lag 24 is not runnable and is skipped by the current implementation.

## `start_end_timestamp_consistency_ratio`

Checks that parseable start timestamps are not later than end timestamps.

- **Implementation:** `cbr_tests/metrics/temporal.py`
- **Supplied-plan usage:** Not used by a supplied plan
- **Inputs:** `start_timestamp_field`, `end_timestamp_field`.
- **Primary output:** Parseable pair, consistent pair, inconsistent pair counts and ratio.
- **Interpretation:** Higher is better.
- **Current caveat:** Rows with either timestamp unparseable are excluded from the ratio.

## `timestamp_parse_success_ratio`

Checks how many values in one timestamp field parse as UTC datetimes.

- **Implementation:** `cbr_tests/metrics/temporal.py`
- **Supplied-plan usage:** `deepsecure_plan`
- **Inputs:** `timestamp_field`, default `timestamp`.
- **Primary output:** Parsed/failed counts and parsed ratio.
- **Interpretation:** Higher is better; 1 means every row parsed.
- **Current caveat:** Parseability does not prove correct timezone, ordering, or semantic accuracy.
