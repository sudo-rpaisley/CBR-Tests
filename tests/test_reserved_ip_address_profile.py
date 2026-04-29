from pathlib import Path

from tests.reserved_ip_address_profile import run_reserved_ip_address_metric


def test_reserved_ip_address_metric_counts(tmp_path: Path):
    data = "Source IP,Destination IP\n192.168.1.10,8.8.8.8\n,999.1.1.1\n2001:db8::1,::1\n"
    dataset = tmp_path / "sample.csv"
    dataset.write_text(data, encoding="utf-8")

    metric = {
        "input_requirements": {
            "candidate_fields": ["Source IP", "Destination IP"]
        }
    }

    success, payload = run_reserved_ip_address_metric(dataset, metric)
    assert success
    result = payload["test_results"]["reserved_ip_address_profile"]

    assert result["row_count"] == 3
    assert result["checked_address_count"] == 6
    assert result["invalid_address_count"] == 1
    assert result["missing_address_count"] == 1
    assert result["reserved_address_count"] == 3
    assert result["reserved_row_count"] == 2
    assert result["status"] == "warn"
    assert result["reserved_category_counts"]["private"] >= 1
    assert result["reserved_category_counts"]["documentation"] >= 1
