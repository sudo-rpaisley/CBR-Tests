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



def run_packet_byte_consistency_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]:
    fm=metric.get("input_requirements",{}).get("field_map",{})
    req=["total_fwd_packets","total_bwd_packets","total_len_fwd_packets","total_len_bwd_packets","fwd_pkt_len_min","fwd_pkt_len_mean","fwd_pkt_len_max","bwd_pkt_len_min","bwd_pkt_len_mean","bwd_pkt_len_max"]
    if any(k not in fm for k in req): return False,{"error":"Missing required fields for packet_byte_consistency_profile.","missing_fields":[k for k in req if k not in fm]}
    try: df=load_tabular_dataset(dataset_path)
    except Exception as exc: return False,{"error":f"Failed to load dataset: {exc}"}
    miss=[fm[k] for k in req if fm[k] not in df.columns]
    if miss: return False,{"error":"Missing required fields for packet_byte_consistency_profile.","missing_fields":miss}
    p=metric.get('calculation',{}).get('parameters',{}); tol=float(p.get('tolerance',1e-6)); vtol=float(p.get('variance_tolerance',1e-3)); max_examples=int(p.get('max_examples',10))
    row_count=len(df); checked=consistent=inconsistent=neg_pkt=neg_byte=order=zero_nonzero=exceed=varmis=invalid=0; examples=[]
    has_std=('packet_length_std' in fm and fm['packet_length_std'] in df.columns); has_var=('packet_length_variance' in fm and fm['packet_length_variance'] in df.columns)
    for idx,row in df.iterrows():
        vals={k:_to_float(row[fm[k]]) for k in req}
        if any(v is None for v in vals.values()): invalid+=1; continue
        checked+=1; bad=False
        if vals['total_fwd_packets']<0 or vals['total_bwd_packets']<0: neg_pkt+=1; bad=True
        if vals['total_len_fwd_packets']<0 or vals['total_len_bwd_packets']<0: neg_byte+=1; bad=True
        if min(vals['fwd_pkt_len_min'],vals['fwd_pkt_len_mean'],vals['fwd_pkt_len_max'],vals['bwd_pkt_len_min'],vals['bwd_pkt_len_mean'],vals['bwd_pkt_len_max'])<0: neg_byte+=1; bad=True
        if not(vals['fwd_pkt_len_min']<=vals['fwd_pkt_len_mean']<=vals['fwd_pkt_len_max']) or not(vals['bwd_pkt_len_min']<=vals['bwd_pkt_len_mean']<=vals['bwd_pkt_len_max']): order+=1; bad=True
        if vals['total_fwd_packets']==0 and vals['total_len_fwd_packets']>0: zero_nonzero+=1; bad=True
        if vals['total_bwd_packets']==0 and vals['total_len_bwd_packets']>0: zero_nonzero+=1; bad=True
        if vals['total_fwd_packets']>0 and vals['total_len_fwd_packets']>vals['total_fwd_packets']*vals['fwd_pkt_len_max']+tol: exceed+=1; bad=True
        if vals['total_bwd_packets']>0 and vals['total_len_bwd_packets']>vals['total_bwd_packets']*vals['bwd_pkt_len_max']+tol: exceed+=1; bad=True
        if has_std and has_var:
            std=_to_float(row[fm['packet_length_std']]); var=_to_float(row[fm['packet_length_variance']])
            if std is not None and var is not None:
                allowed=max(vtol, abs(var)*vtol)
                if abs(var-(std**2))>allowed: varmis+=1; bad=True
        if bad: inconsistent+=1; examples.append({"row_index":int(idx),"reason":"packet_byte_inconsistency"}) if len(examples)<max_examples else None
        else: consistent+=1
    ratio=round(consistent/checked,6) if checked else 0.0
    status="pass" if ratio>=0.99 else "warn" if ratio>=0.95 else "fail"
    return True,{"test_results":{"packet_byte_consistency_profile":{"row_count":row_count,"checked_row_count":checked,"consistent_row_count":consistent,"inconsistent_row_count":inconsistent,"negative_packet_count":neg_pkt,"negative_byte_count":neg_byte,"length_order_violation_count":order,"zero_packet_nonzero_byte_count":zero_nonzero,"byte_total_exceeds_max_possible_count":exceed,"variance_std_mismatch_count":varmis,"invalid_numeric_row_count":invalid,"packet_byte_consistency_ratio":ratio,"examples":examples,"status":status}}}
