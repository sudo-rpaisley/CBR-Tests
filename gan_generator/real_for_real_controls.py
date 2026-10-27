"""Read-only-input real-for-real PCAP substitution controls.

These tests estimate normal session variation; they do not create synthetic
traffic. Packet-wise substitution is a deliberately weak control. Whole-flow
substitution keeps the donor flow's packets together but does not establish
application continuity with the retained target flows.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil

import numpy as np


def sha256(path: Path) -> str:
    h=hashlib.sha256()
    with path.open("rb") as f:
        for part in iter(lambda:f.read(1024*1024),b""):
            h.update(part)
    return h.hexdigest()


def flow_key(packet):
    from scapy.layers.inet import IP, TCP, UDP
    from scapy.layers.inet6 import IPv6
    if not (IP in packet or IPv6 in packet):
        return ("non-ip",)
    ip = packet[IP] if IP in packet else packet[IPv6]
    proto = 6 if TCP in packet else 17 if UDP in packet else int(getattr(ip,"proto",getattr(ip,"nh",0)))
    port_src = int(packet[TCP].sport) if TCP in packet else int(packet[UDP].sport) if UDP in packet else 0
    port_dst = int(packet[TCP].dport) if TCP in packet else int(packet[UDP].dport) if UDP in packet else 0
    a,b = (str(ip.src),port_src),(str(ip.dst),port_dst)
    return (proto,min(a,b),max(a,b))


def load_pcap(path: Path, max_packets: int):
    from scapy.utils import PcapReader
    frames=[]
    with PcapReader(str(path)) as reader:
        for frame in reader:
            if len(frames)>=max_packets:
                raise ValueError(f"PCAP has more than max_packets={max_packets}; use smaller pilot or increase explicit limit")
            frames.append(frame)
    return frames


def replace_real_with_real(target: Path, donor: Path, output: Path, percentage: int,
                           mode: str = "flow", placement: str = "distributed",
                           seed: int = 42, max_packets: int = 200_000) -> dict:
    from scapy.utils import PcapWriter
    if not 0 <= percentage <= 100:
        raise ValueError("percentage must be 0..100")
    if target.resolve()==donor.resolve() or sha256(target)==sha256(donor):
        raise ValueError("Donor must be a different capture")
    if output.exists() or output.resolve() in {target.resolve(),donor.resolve()}:
        raise FileExistsError("Output exists or matches an input")
    if mode not in {"packet","flow"} or placement not in {"distributed","trailing"}:
        raise ValueError("Unsupported mode or placement")
    target_packets=load_pcap(target,max_packets)
    donor_packets=load_pcap(donor,max_packets)
    if not target_packets or not donor_packets:
        raise ValueError("Both PCAPs require packets")
    if type(target_packets[0]) is not type(donor_packets[0]):
        raise ValueError("Capture link-layer packet types differ")
    rng=np.random.default_rng(seed)
    selected=set()
    desired=round(len(target_packets)*percentage/100)
    if mode=="packet":
        if placement=="trailing":
            selected=set(range(len(target_packets)-desired,len(target_packets)))
        else:
            selected=set(map(int,rng.choice(len(target_packets),size=desired,replace=False)))
        indices=map(int,rng.choice(len(donor_packets),size=len(selected),replace=True))
        substitute = {i:donor_packets[j].copy() for i,j in zip(sorted(selected),indices)}
        for i,frame in substitute.items():
            frame.time=target_packets[i].time
        frames=[substitute.get(i,frame) for i,frame in enumerate(target_packets)]
    else:
        tgroups=defaultdict(list)
        dgroups=defaultdict(list)
        for i,p in enumerate(target_packets):tgroups[flow_key(p)].append(i)
        for i,p in enumerate(donor_packets):dgroups[flow_key(p)].append(i)
        target_groups=list(tgroups)
        if placement=="distributed":
            rng.shuffle(target_groups)
        else:
            target_groups.sort(key=lambda key:tgroups[key][0],reverse=True)
        for key in target_groups:
            if len(selected)>=desired:
                break
            selected.update(tgroups[key])
        # Require donor flows with the same transport protocol when available.
        donor_keys=list(dgroups)
        kept=[p for i,p in enumerate(target_packets) if i not in selected]
        substitutions=[]
        for key in target_groups:
            if not any(i in selected for i in tgroups[key]):
                continue
            eligible=[k for k in donor_keys if k[0]==key[0]]
            if not eligible:
                raise ValueError(f"No matching donor transport for flow {key}")
            choice=eligible[int(rng.integers(0,len(eligible)))]
            start=float(target_packets[tgroups[key][0]].time)
            donor_start=float(donor_packets[dgroups[choice][0]].time)
            for di in dgroups[choice]:
                cloned=donor_packets[di].copy()
                cloned.time=start+max(0,float(donor_packets[di].time)-donor_start)
                substitutions.append(cloned)
        frames=kept+substitutions
        frames.sort(key=lambda p:float(p.time))
    output.parent.mkdir(parents=True,exist_ok=True)
    if percentage == 0:
        # True null control: preserve original bytes, metadata, ordering and
        # file format rather than rewriting/sorting a nominal 0% capture.
        shutil.copyfile(target, output)
        frames = target_packets
    else:
        try:
            with PcapWriter(str(output),sync=True) as writer:
                for p in frames:writer.write(p)
        except Exception:
            output.unlink(missing_ok=True)
            raise
    manifest={
        "control_type":"real_for_real",
        "mode":mode,"placement":placement,"seed":seed,
        "target_capture":str(target.resolve()),"target_sha256":sha256(target),
        "donor_capture":str(donor.resolve()),"donor_sha256":sha256(donor),
        "output_capture":str(output.resolve()),"output_sha256":sha256(output),
        "requested_target_packet_fraction":percentage,
        "selected_target_packet_count":len(selected),
        "selected_target_packet_fraction":len(selected)/len(target_packets),
        "original_target_packet_count":len(target_packets),
        "output_packet_count":len(frames),
        "same_packet_count":len(target_packets)==len(frames),
        "created_at_utc":datetime.now(timezone.utc).isoformat(),
        "limitations":"No claim of realism degradation; flow-mode retains donor packets as flows but not shared application context."
    }
    output.with_suffix(output.suffix+".json").write_text(json.dumps(manifest,indent=2)+"\n")
    return manifest


def main(argv=None):
    parser=argparse.ArgumentParser()
    parser.add_argument("--target",type=Path,required=True)
    parser.add_argument("--donor",type=Path,required=True)
    parser.add_argument("--out",type=Path,required=True)
    parser.add_argument("--percentages",default="0,10,25,50,75,100")
    parser.add_argument("--mode",choices=["packet","flow"],default="flow")
    parser.add_argument("--placement",choices=["distributed","trailing"],default="distributed")
    parser.add_argument("--seed",type=int,default=42)
    parser.add_argument("--max-packets",type=int,default=200000)
    args=parser.parse_args(argv)
    for pct in map(int,args.percentages.split(",")):
        dest=args.out/f"real_for_real_{args.mode}_{pct:03d}.pcap"
        result=replace_real_with_real(args.target,args.donor,dest,pct,
                                      args.mode,args.placement,args.seed,args.max_packets)
        print(json.dumps({"output":result["output_capture"],"requested_pct":pct,
                          "actual_target_fraction":result["selected_target_packet_fraction"]}))
    return 0


if __name__=="__main__":
    raise SystemExit(main())
