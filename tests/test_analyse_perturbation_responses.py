import pandas as pd
import pytest

from scripts.analyse_perturbation_responses import load_rows, curves, summaries


def fixture_rows():
    rows=[]
    for capture in ("a.pcap", "b.pcap", "c.pcap"):
        for pct in (0,10,50,100):
            rows.append({"group":"cloud","held_out":capture,"seed":42,
                         "metric_id":"ks","perturbation_pct":pct,
                         "primary_value":pct/100,
                         "execution_status":"success","attempt_status":"completed"})
    return pd.DataFrame(rows)


def test_response_slopes_and_clustered_summaries():
    df=fixture_rows()
    responses=curves(df)
    assert len(responses)==3
    assert responses["monotonic_non_decreasing"].all()
    assert (responses["delta_final_from_zero"]==1).all()
    assert (responses["complete_11_level_series"]==False).all()
    grouped=summaries(responses)
    assert grouped.iloc[0]["held_out_capture_count"]==3
    assert grouped.iloc[0]["median_capture_level_delta"]==pytest.approx(1)
    assert grouped.iloc[0]["bootstrap_capture_cluster_ci_2_5"]==pytest.approx(1)


def test_duplicate_attempts_fail_closed(tmp_path):
    data=fixture_rows()
    data=pd.concat([data,data.iloc[:1]],ignore_index=True)
    path=tmp_path/"metrics.csv"
    data.to_csv(path,index=False)
    with pytest.raises(ValueError,match="Duplicate metric outcomes"):
        load_rows(path)


def test_non_numeric_and_failed_rows_removed(tmp_path):
    data=fixture_rows()
    data.loc[0,"execution_status"]="failed"
    data["primary_value"] = data["primary_value"].astype(object)
    data.loc[1,"primary_value"]="not-applicable"
    path=tmp_path/"metrics.csv"
    data.to_csv(path,index=False)
    result=load_rows(path)
    assert len(result)==len(data)-2
