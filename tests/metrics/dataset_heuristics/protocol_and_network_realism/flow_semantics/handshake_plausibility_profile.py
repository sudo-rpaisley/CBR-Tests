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



def run_handshake_plausibility_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]:
    field_map=metric.get("input_requirements",{}).get("field_map",{})
    req=["protocol","total_fwd_packets","total_bwd_packets","syn_flag_count","ack_flag_count","rst_flag_count","fin_flag_count"]
    if any(k not in field_map for k in req): return False,{"error":"Missing required fields for handshake_plausibility_profile.","missing_fields":[k for k in req if k not in field_map]}
    try: df=load_tabular_dataset(dataset_path)
    except Exception as exc: return False,{"error":f"Failed to load dataset: {exc}"}
    miss=[field_map[k] for k in req if field_map[k] not in df.columns]
    if miss: return False,{"error":"Missing required fields for handshake_plausibility_profile.","missing_fields":miss}
    p=metric.get("calculation",{}).get("parameters",{})
    tcp_vals={str(v).lower() for v in p.get("tcp_protocol_values",[6,"6","TCP","tcp"]) }
    allow_syn_only=bool(p.get("allow_syn_only",False)); allow_rst=bool(p.get("allow_rst_flows",True)); max_examples=int(p.get("max_examples",10))
    row_count=len(df); tcp_rows=checked=plaus=susp=uncertain=syn_only=synack=ack_wo=rst_count=0; examples=[]
    for idx,row in df.iterrows():
        if str(row[field_map['protocol']]).strip().lower() not in tcp_vals: continue
        tcp_rows+=1
        vals={k:_to_float(row[field_map[k]]) for k in ["total_fwd_packets","total_bwd_packets","syn_flag_count","ack_flag_count","rst_flag_count","fin_flag_count"]}
        if any(v is None for v in vals.values()): continue
        checked+=1
        total=vals['total_fwd_packets']+vals['total_bwd_packets']; syn=vals['syn_flag_count']; ack=vals['ack_flag_count']; rst=vals['rst_flag_count']
        if rst>0 and allow_rst: plaus+=1; rst_count+=1; continue
        if syn>0 and ack>0: plaus+=1; synack+=1; continue
        if syn>0 and ack==0:
            syn_only+=1
            if allow_syn_only: plaus+=1
            else: susp+=1; examples.append({"row_index":int(idx),"reason":"syn_only"}) if len(examples)<max_examples else None
            continue
        if ack>0 and syn==0: ack_wo+=1; susp+=1; examples.append({"row_index":int(idx),"reason":"ack_without_syn"}) if len(examples)<max_examples else None; continue
        if total>1 and syn==0 and ack==0: susp+=1; examples.append({"row_index":int(idx),"reason":"no_syn_no_ack_multi_packet"}) if len(examples)<max_examples else None
        else: uncertain+=1
    if tcp_rows==0:
        return True,{"test_results":{"handshake_plausibility_profile":{"row_count":row_count,"tcp_row_count":0,"checked_tcp_row_count":0,"plausible_tcp_row_count":0,"suspicious_tcp_row_count":0,"uncertain_tcp_row_count":0,"syn_only_count":0,"syn_ack_like_count":0,"ack_without_syn_count":0,"rst_flow_count":0,"handshake_plausibility_ratio":0.0,"examples":[],"status":"not_applicable"}}}
    ratio=round(plaus/checked,6) if checked else 0.0
    status="pass" if ratio>=0.95 else "warn" if ratio>=0.80 else "fail"
    return True,{"test_results":{"handshake_plausibility_profile":{"row_count":row_count,"tcp_row_count":tcp_rows,"checked_tcp_row_count":checked,"plausible_tcp_row_count":plaus,"suspicious_tcp_row_count":susp,"uncertain_tcp_row_count":uncertain,"syn_only_count":syn_only,"syn_ack_like_count":synack,"ack_without_syn_count":ack_wo,"rst_flow_count":rst_count,"handshake_plausibility_ratio":ratio,"examples":examples[:max_examples],"status":status}}}
