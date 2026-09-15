from runner.metric_catalog import build_metric_catalog


def test_slice_realism_context_is_not_inherited_from_saved_plan_templates():
    catalogue = {entry["metric_id"]: entry for entry in build_metric_catalog()}

    sample = catalogue["per_slice_sample_coverage_ratio"]
    assert sample["manual_configuration_reason"] == "expected_slice_ids_required"
    if sample["template"] is not None:
        assert sample["template"]["input_requirements"]["expected_slice_ids"] == []

    classes = catalogue["per_slice_class_coverage_ratio"]
    assert classes["manual_configuration_reason"] == "expected_classes_required"
    if classes["template"] is not None:
        assert classes["template"]["input_requirements"]["expected_classes"] == []

    leakage = catalogue["cross_slice_identifier_leakage_ratio"]
    assert leakage["manual_configuration_reason"] == "slice_exclusivity_policy_required"
    if leakage["template"] is not None:
        assert leakage["template"]["input_requirements"]["identifier_fields"] == []
        assert leakage["template"]["calculation"]["parameters"]["expect_slice_exclusive"] is False
