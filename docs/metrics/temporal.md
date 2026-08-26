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

Measures day-to-day similarity of UTC hour-of-day activity shapes. One 24-bin packet/row-count vector is built for each observed calendar day, then cosine similarity is calculated for every day pair and averaged.

- **Implementation:** `cbr_tests/metrics/temporal.py`
- **Supplied-plan usage:** `deepsecure_plan`
- **Inputs:** `timestamp_field`; `minimum_day_count` default 2.
- **Primary output:** Observed-day count, day-pair count, runnable flag and mean pairwise similarity score.
- **Interpretation:** Closer to 1 means the daily hour-of-day activity shape is more stable across observed days.
- **Current caveat:** This is an internal temporal-stability measure, not external realism by itself. Calendar-day profiles use UTC after timestamp parsing; captures spanning only one observed day are reported as not evaluable rather than assigned a misleading score.

## `hourly_activity_distribution_divergence`

Measures day-to-day divergence of UTC hour-of-day activity distributions. One 24-bin distribution is built for each observed calendar day and total-variation distance is averaged across every day pair.

- **Implementation:** `cbr_tests/metrics/temporal.py`
- **Supplied-plan usage:** `deepsecure_plan`
- **Inputs:** `timestamp_field`; `minimum_day_count` default 2.
- **Primary output:** Observed-day count, day-pair count, runnable flag and divergence in [0,1].
- **Interpretation:** 0 means identical daily hourly proportions; 1 means the compared daily profiles occupy disjoint hours.
- **Current caveat:** This is an internal stability measure and is sensitive to timezone/capture coverage. It no longer splits packets chronologically, which previously made a regular single-day capture appear maximally divergent merely because different clock hours fell into different halves.

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

Measures repeat similarity at configured lags on the actual continuous hourly activity series. The timestamps are binned into consecutive UTC hours, including zero-count hours, and each series is compared with a lagged copy using normalised absolute count difference.

- **Implementation:** `cbr_tests/metrics/temporal.py`
- **Supplied-plan usage:** `deepsecure_plan`.
- **Inputs:** `timestamp_field`; `lags` default `[24]`; `minimum_lag_pairs` default 2.
- **Primary output:** Per-lag paired-hour count, runnable flag and repeat similarity, plus the mean score when every configured lag is evaluable.
- **Interpretation:** 1 means the hourly activity repeats exactly at every configured lag; lower values mean less repeated structure. Lag 24 directly compares each hour with the corresponding hour one day later.
- **Current caveat:** A configured lag is not evaluable when the capture does not span enough hourly bins. The overall score is then `null` rather than silently dropping that lag. This remains an internal periodicity measure rather than reference fidelity.

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
