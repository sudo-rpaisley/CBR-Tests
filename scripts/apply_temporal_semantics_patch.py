from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    target = Path(path)
    text = target.read_text(encoding="utf-8")
    if old not in text:
        raise SystemExit(f"Expected text not found in {path}: {old[:100]!r}")
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


replace_once(
    "runner/pcap_adapter.py",
    '''        "hourly_activity_distribution_divergence": {
            "metric_id": "hourly_activity_distribution_divergence",
            "label": "Packet Hourly Activity Divergence",
            "input_requirements": {"timestamp_field": "Timestamp"},
            "calculation": {
                "method": "Compare first- and second-half UTC hourly packet activity distributions.",
                "parameters": {"timestamp_unit": "s"},
            },
        },
        "diurnal_pattern_similarity_score": {
            "metric_id": "diurnal_pattern_similarity_score",
            "label": "Packet Diurnal Pattern Similarity",
            "input_requirements": {"timestamp_field": "Timestamp"},
            "calculation": {
                "method": "Compare first- and second-half UTC hourly packet-count shapes using cosine similarity.",
                "parameters": {"timestamp_unit": "s"},
            },
        },
        "periodicity_preservation_score": {
            "metric_id": "periodicity_preservation_score",
            "label": "Packet Periodicity Preservation Score",
            "input_requirements": {"timestamp_field": "Timestamp"},
            "calculation": {
                "method": "Compare autocorrelation of first- and second-half UTC hourly packet counts at configured lags.",
                "parameters": {"timestamp_unit": "s", "lags": [1, 24]},
            },
        },
''',
    '''        "hourly_activity_distribution_divergence": {
            "metric_id": "hourly_activity_distribution_divergence",
            "label": "Packet Day-to-Day Hourly Activity Divergence",
            "input_requirements": {"timestamp_field": "Timestamp"},
            "calculation": {
                "method": "Measure mean pairwise total-variation divergence between UTC hour-of-day activity profiles from separate observed days.",
                "parameters": {"timestamp_unit": "s", "minimum_day_count": 2},
            },
        },
        "diurnal_pattern_similarity_score": {
            "metric_id": "diurnal_pattern_similarity_score",
            "label": "Packet Day-to-Day Diurnal Pattern Similarity",
            "input_requirements": {"timestamp_field": "Timestamp"},
            "calculation": {
                "method": "Measure mean pairwise cosine similarity between UTC hour-of-day packet-count profiles from separate observed days.",
                "parameters": {"timestamp_unit": "s", "minimum_day_count": 2},
            },
        },
        "periodicity_preservation_score": {
            "metric_id": "periodicity_preservation_score",
            "label": "Packet Hourly Periodicity Repeat Similarity",
            "input_requirements": {"timestamp_field": "Timestamp"},
            "calculation": {
                "method": "Compare the continuous UTC hourly packet-count series with lagged copies; lag 24 directly measures one-day repeat similarity.",
                "parameters": {"timestamp_unit": "s", "lags": [24], "minimum_lag_pairs": 2},
            },
        },
''',
)

replace_once(
    "docs/metrics/temporal.md",
    '''## `diurnal_pattern_similarity_score`

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
''',
    '''## `diurnal_pattern_similarity_score`

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
''',
)

replace_once(
    "docs/metrics/temporal.md",
    '''## `periodicity_preservation_score`

Compares autocorrelation at configured lags between timestamp halves.

- **Implementation:** `cbr_tests/metrics/temporal.py`
- **Supplied-plan usage:** `deepsecure_plan`
- **Inputs:** `timestamp_field`; `lags` default `[1,24]`.
- **Primary output:** Per-lag autocorrelations/deviations and 1 minus mean capped deviation.
- **Interpretation:** Higher means more similar autocorrelation structure.
- **Current caveat:** Hourly count vectors always have length 24, so lag 24 is not runnable and is skipped by the current implementation.
''',
    '''## `periodicity_preservation_score`

Measures repeat similarity at configured lags on the actual continuous hourly activity series. The timestamps are binned into consecutive UTC hours, including zero-count hours, and each series is compared with a lagged copy using normalised absolute count difference.

- **Implementation:** `cbr_tests/metrics/temporal.py`
- **Supplied-plan usage:** `deepsecure_plan`.
- **Inputs:** `timestamp_field`; `lags` default `[24]`; `minimum_lag_pairs` default 2.
- **Primary output:** Per-lag paired-hour count, runnable flag and repeat similarity, plus the mean score when every configured lag is evaluable.
- **Interpretation:** 1 means the hourly activity repeats exactly at every configured lag; lower values mean less repeated structure. Lag 24 directly compares each hour with the corresponding hour one day later.
- **Current caveat:** A configured lag is not evaluable when the capture does not span enough hourly bins. The overall score is then `null` rather than silently dropping that lag. This remains an internal periodicity measure rather than reference fidelity.
''',
)
