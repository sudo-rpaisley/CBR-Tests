from pathlib import Path
import pandas as pd
import math


def load_tabular_dataset(dataset_path: Path) -> pd.DataFrame:
    suffix = dataset_path.suffix.lower()

    if suffix == ".csv":
        df = pd.read_csv(dataset_path, skipinitialspace=True, low_memory=False)
    elif suffix == ".tsv":
        df = pd.read_csv(dataset_path, sep="\t", skipinitialspace=True, low_memory=False)
    elif suffix in [".xlsx", ".xls"]:
        df = pd.read_excel(dataset_path)
    else:
        raise ValueError(f"Unsupported tabular dataset format: {suffix}")

    df.columns = df.columns.str.strip()
    return df


def _to_float(v):
    if pd.isna(v):
        return None
    s=str(v).strip()
    if s=="":
        return None
    try:
        return float(s)
    except Exception:
        return None



def run_tcp_flag_consistency_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]:
    field_map = metric.get("input_requirements", {}).get("field_map", {})
    required=["protocol","total_fwd_packets","total_bwd_packets","syn_flag_count","ack_flag_count","fin_flag_count","rst_flag_count"]
    missing_keys=[k for k in required if k not in field_map]
    if missing_keys:
        return False,{"error":"Missing required fields for tcp_flag_consistency_profile.","missing_fields":missing_keys}
    try: df=load_tabular_dataset(dataset_path)
    except Exception as exc: return False,{"error":f"Failed to load dataset: {exc}"}
    missing_fields=[field_map[k] for k in required if field_map[k] not in df.columns]
    if missing_fields: return False,{"error":"Missing required fields for tcp_flag_consistency_profile.","missing_fields":missing_fields}
    params=metric.get("calculation",{}).get("parameters",{})
    tcp_vals={str(v).lower() for v in params.get("tcp_protocol_values",[6,"6","TCP","tcp"]) }
    non_tcp_zero=bool(params.get("non_tcp_flags_must_be_zero",True)); max_examples=int(params.get("max_examples",10))
    flag_keys=["fin_flag_count","syn_flag_count","rst_flag_count","ack_flag_count","psh_flag_count","urg_flag_count","cwe_flag_count","ece_flag_count"]
    present=[k for k in flag_keys if k in field_map and field_map[k] in df.columns]
    row_count=len(df); checked=consistent=inconsistent=tcp_rows=non_tcp_rows=0
    negative=exceeds=non_tcp_with=invalid_numeric=0; examples=[]
    for idx,row in df.iterrows():
        proto=str(row[field_map["protocol"]]).strip().lower()
        is_tcp=proto in tcp_vals
        total_fwd=_to_float(row[field_map["total_fwd_packets"]]); total_bwd=_to_float(row[field_map["total_bwd_packets"]])
        if total_fwd is None or total_bwd is None: invalid_numeric+=1; continue
        total=total_fwd+total_bwd
        bad=False; non_zero=False
        for k in present:
            val=_to_float(row[field_map[k]])
            if val is None: bad=True; invalid_numeric+=1; break
            if val<0: negative+=1; bad=True
            if val>total: exceeds+=1; bad=True
            if val>0: non_zero=True
        if bad:
            inconsistent+=1; checked+=1
            if len(examples)<max_examples: examples.append({"row_index":int(idx),"reason":"invalid_or_out_of_bounds_flags"})
            continue
        if is_tcp: tcp_rows+=1
        else: non_tcp_rows+=1
        if (not is_tcp) and non_tcp_zero and non_zero:
            non_tcp_with+=1; inconsistent+=1
            if len(examples)<max_examples: examples.append({"row_index":int(idx),"reason":"non_tcp_with_tcp_flags"})
        else: consistent+=1
        checked+=1
    if checked==0: return False,{"error":"No rows could be checked for tcp_flag_consistency_profile."}
    ratio=round(consistent/checked,6)
    status="pass" if ratio>=0.99 else "warn" if ratio>=0.95 else "fail"
    return True,{"test_results":{"tcp_flag_consistency_profile":{"row_count":row_count,"checked_row_count":checked,"consistent_row_count":consistent,"inconsistent_row_count":inconsistent,"tcp_row_count":tcp_rows,"non_tcp_row_count":non_tcp_rows,"tcp_flag_consistency_ratio":ratio,"negative_flag_count":negative,"flag_exceeds_packet_count":exceeds,"non_tcp_with_tcp_flags_count":non_tcp_with,"invalid_numeric_row_count":invalid_numeric,"examples":examples,"status":status}}}
