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



def run_flow_duration_consistency_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]:
    fm=metric.get("input_requirements",{}).get("field_map",{})
    req=["flow_duration","flow_iat_mean","flow_iat_max","flow_iat_min"]
    if any(k not in fm for k in req): return False,{"error":"Missing required fields for flow_duration_consistency_profile.","missing_fields":[k for k in req if k not in fm]}
    try: df=load_tabular_dataset(dataset_path)
    except Exception as exc: return False,{"error":f"Failed to load dataset: {exc}"}
    miss=[fm[k] for k in req if fm[k] not in df.columns]
    if miss: return False,{"error":"Missing required fields for flow_duration_consistency_profile.","missing_fields":miss}
    tol=float(metric.get('calculation',{}).get('parameters',{}).get('tolerance',1e-6)); max_examples=int(metric.get('calculation',{}).get('parameters',{}).get('max_examples',10))
    row_count=len(df); checked=consistent=inconsistent=neg_dur=neg_iat=order_v=iat_ex=dir_ex=invalid=0; examples=[]
    opt=[k for k in ["flow_iat_std","fwd_iat_total","bwd_iat_total"] if k in fm and fm[k] in df.columns]
    for idx,row in df.iterrows():
        vals={k:_to_float(row[fm[k]]) for k in req+opt}
        if any(vals[k] is None for k in req): invalid+=1; continue
        checked+=1; bad=False
        if vals['flow_duration']<0: neg_dur+=1; bad=True
        if vals['flow_iat_min']<0 or vals['flow_iat_mean']<0 or vals['flow_iat_max']<0 or ('flow_iat_std' in vals and vals['flow_iat_std'] is not None and vals['flow_iat_std']<0): neg_iat+=1; bad=True
        if not(vals['flow_iat_min']<=vals['flow_iat_mean']<=vals['flow_iat_max']): order_v+=1; bad=True
        if vals['flow_iat_max']>vals['flow_duration']+tol: iat_ex+=1; bad=True
        if 'fwd_iat_total' in vals and vals['fwd_iat_total'] is not None and vals['fwd_iat_total']>vals['flow_duration']+tol: dir_ex+=1; bad=True
        if 'bwd_iat_total' in vals and vals['bwd_iat_total'] is not None and vals['bwd_iat_total']>vals['flow_duration']+tol: dir_ex+=1; bad=True
        if bad: inconsistent+=1; examples.append({"row_index":int(idx),"reason":"duration_iat_violation"}) if len(examples)<max_examples else None
        else: consistent+=1
    ratio=round(consistent/checked,6) if checked else 0.0
    status="pass" if ratio>=0.99 else "warn" if ratio>=0.95 else "fail"
    return True,{"test_results":{"flow_duration_consistency_profile":{"row_count":row_count,"checked_row_count":checked,"consistent_row_count":consistent,"inconsistent_row_count":inconsistent,"negative_duration_count":neg_dur,"negative_iat_count":neg_iat,"iat_order_violation_count":order_v,"iat_exceeds_duration_count":iat_ex,"direction_iat_exceeds_duration_count":dir_ex,"invalid_numeric_row_count":invalid,"flow_duration_consistency_ratio":ratio,"examples":examples,"status":status}}}
