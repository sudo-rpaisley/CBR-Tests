"""Bounded-memory CTGAN PCAP/PCAPNG trailing-suffix generator.

Unlike the archived prototype, no input capture, list of Scapy packets, or
reconstructed capture is materialised in RAM. A disk-backed numeric plan holds
only packet lengths; container records and labels are streamed independently.
The generator preserves byte-balanced incremental suffix bands and deterministic
nested replacements for a fixed seed and complete sweep level set.
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import pickle
import random
import shutil
import struct
import tempfile
import time
from dataclasses import dataclass
from contextlib import contextmanager
from pathlib import Path

from tail_replacement import CaptureError, ipv4_offset
from variable_replacement import propose_length, synthesize_variable_length

COLUMNS = ["protocol", "service_port", "packet_count_log", "duration_ms_log",
           "mean_ip_length", "forward_fraction", "mean_ttl"]
COMMON_SERVICES = {20,21,22,25,53,67,68,80,110,123,143,161,389,443,445,465,
                   514,587,636,853,993,995,1433,1883,3306,3389,5060,5432,
                   5672,8080,8443,8883}
PCAP_ENDIAN = {b'\xd4\xc3\xb2\xa1': '<', b'\xa1\xb2\xc3\xd4': '>',
               b'\x4d\x3c\xb2\xa1': '<', b'\xa1\xb2\x3c\x4d': '>'}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for part in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(part)
    return h.hexdigest()


def _u32(raw, pos, endian):
    return struct.unpack_from(endian + 'I', raw, pos)[0]


def _section_options(block, start, stop, endian):
    out = {}
    pos = start
    while pos + 4 <= stop:
        code, size = struct.unpack_from(endian + 'HH', block, pos)
        if code == 0:
            break
        value_start = pos + 4
        if value_start + size > stop:
            raise CaptureError('Malformed PCAPNG interface options')
        out[code] = block[value_start:value_start + size]
        pos = value_start + ((size + 3) & ~3)
    return out


@dataclass
class Record:
    raw: bytes
    packet: bytes | None = None
    index: int | None = None
    linktype: int | None = None
    time: float = 0.0
    endian: str = '<'
    kind: str = ''
    packet_offset: int = 0

    def rewrite(self, updated: bytes) -> bytes:
        if self.packet is None:
            raise ValueError('Not a packet record')
        if self.kind == 'pcap':
            head = bytearray(self.raw[:16])
            struct.pack_into(self.endian + 'II', head, 8, len(updated), len(updated))
            return bytes(head) + updated
        if self.kind not in {'epb', 'pb'}:
            raise ValueError('Unexpected packet block kind')
        old_pad = (len(self.packet) + 3) & ~3
        tail = self.raw[self.packet_offset + old_pad:-4]
        head = bytearray(self.raw[:self.packet_offset])
        struct.pack_into(self.endian + 'II', head, 20, len(updated), len(updated))
        size = len(head) + ((len(updated) + 3) & ~3) + len(tail) + 4
        struct.pack_into(self.endian + 'I', head, 4, size)
        return (bytes(head) + updated + b'\x00' * (-len(updated) % 4) + tail
                + struct.pack(self.endian + 'I', size))


def iter_records(path: Path):
    """Bounded per-record parser; retain byte-identical non-selected records.

    PCAPNG timestamp resolution / offset are handled per interface and section.
    Hash-bearing packet options must be rejected to avoid stale hash metadata.
    """
    with Path(path).open('rb') as handle:
        magic = handle.read(4)
        handle.seek(0)
        index = 0
        if magic in PCAP_ENDIAN:
            endian = PCAP_ENDIAN[magic]
            header = handle.read(24)
            if len(header) < 24:
                raise CaptureError('Truncated PCAP global header')
            snaplen = _u32(header, 16, endian)
            linktype = _u32(header, 20, endian) & 0xffff
            nano = magic in {b'\x4d\x3c\xb2\xa1', b'\xa1\xb2\x3c\x4d'}
            yield Record(raw=header, kind='header', endian=endian)
            while True:
                head = handle.read(16)
                if not head:
                    break
                if len(head) != 16:
                    raise CaptureError('Truncated PCAP record header')
                sec, frac, caplen, origlen = struct.unpack(endian + 'IIII', head)
                if caplen > origlen or caplen > 100_000_000:
                    offset = handle.tell() - len(head)
                    raise CaptureError(
                        f'Invalid PCAP record {index} at byte offset {offset} '
                        f'(captured_length={caplen}, original_length={origlen}, '
                        f'global_snaplen={snaplen}, file={path}). '
                        'The capture must be inspected or repaired before inclusion.'
                    )
                # Zero-length PCAP records are representable and can be kept
                # intact; they are unsupported for synthetic replacement.
                packet = handle.read(caplen)
                if len(packet) != caplen:
                    raise CaptureError('Truncated PCAP packet')
                yield Record(raw=head + packet, packet=packet, index=index,
                             linktype=linktype, time=sec + frac / (1e9 if nano else 1e6),
                             endian=endian, kind='pcap', packet_offset=16)
                index += 1
            return
        if magic != b'\x0a\x0d\x0d\x0a':
            raise CaptureError('Not a PCAP or PCAPNG capture')
        endian = None
        interfaces = []
        while True:
            prefix = handle.read(12)
            if not prefix:
                break
            if len(prefix) != 12:
                raise CaptureError('Truncated PCAPNG block header')
            if prefix[:4] == b'\x0a\x0d\x0d\x0a':
                bom = prefix[8:12]
                endian = '<' if bom == b'\x4d\x3c\x2b\x1a' else '>' if bom == b'\x1a\x2b\x3c\x4d' else None
                interfaces = []
            if endian is None:
                raise CaptureError('Invalid PCAPNG section byte order')
            length = _u32(prefix, 4, endian)
            if length < 12 or length % 4 or length > 100_000_000:
                raise CaptureError(f'Invalid PCAPNG block length: {length}')
            body = handle.read(length - 12)
            if len(body) != length - 12:
                raise CaptureError('Truncated PCAPNG block')
            raw = prefix + body
            if _u32(raw, length - 4, endian) != length:
                raise CaptureError('PCAPNG trailing block length mismatch')
            typ = _u32(raw, 0, endian)
            if typ == 1:
                if length < 20:
                    raise CaptureError('Truncated PCAPNG IDB')
                options = _section_options(raw, 16, length - 4, endian)
                value = options.get(9, b'\x06')[:1][0]
                resolution = (2.0 ** -(value & 0x7f) if value & 0x80
                              else 10.0 ** -value)
                offset_data = options.get(14)
                offset = struct.unpack(endian + 'q', offset_data)[0] if offset_data and len(offset_data) == 8 else 0
                interfaces.append((_u32(raw, 8, endian) & 0xffff, resolution, offset))
            elif typ in (2, 6):
                if length < 32:
                    raise CaptureError('Truncated PCAPNG packet block')
                interface = (_u32(raw, 8, endian) if typ == 6
                             else struct.unpack_from(endian + 'H', raw, 8)[0])
                if interface >= len(interfaces):
                    raise CaptureError('Unknown PCAPNG interface')
                caplen = _u32(raw, 20, endian)
                origlen = _u32(raw, 24, endian)
                packet_start = 28
                opt_start = packet_start + ((caplen + 3) & ~3)
                if not caplen or origlen < caplen or opt_start > length - 4:
                    raise CaptureError('Invalid PCAPNG captured packet length')
                options = _section_options(raw, opt_start, length - 4, endian)
                if 3 in options:
                    raise CaptureError('PCAPNG packet hash option must be removed before reconstruction')
                linktype, resolution, offset = interfaces[interface]
                hi = _u32(raw, 12, endian)
                low = _u32(raw, 16, endian)
                timestamp = (hi * (1 << 32) + low) * resolution + offset
                yield Record(raw=raw, packet=raw[packet_start:packet_start+caplen],
                             index=index, linktype=linktype, time=timestamp,
                             endian=endian, kind='epb' if typ == 6 else 'pb',
                             packet_offset=packet_start)
                index += 1
                continue
            elif typ == 3:
                raise CaptureError('PCAPNG Simple Packet Blocks not supported')
            yield Record(raw=raw, kind='block', endian=endian)


@dataclass
class FlowSummary:
    uid: int
    protocol: str
    endpoint_a: tuple
    endpoint_b: tuple
    first: float
    last: float
    first_packet_index: int = 0
    packet_count: int = 0
    length_sum: int = 0
    ttl_sum: int = 0
    forward_sum: int = 0

    def features(self):
        port = self.endpoint_b[1]
        return {
            'protocol': self.protocol,
            'service_port': str(port) if port in COMMON_SERVICES else 'other',
            'packet_count_log': math.log1p(self.packet_count),
            'duration_ms_log': math.log1p(max(0, self.last - self.first) * 1000),
            'mean_ip_length': self.length_sum / self.packet_count,
            'forward_fraction': self.forward_sum / self.packet_count,
            'mean_ttl': self.ttl_sum / self.packet_count,
        }


class FlowTracker:
    def __init__(self, idle_timeout=60.0):
        self.idle_timeout = idle_timeout
        self.flows = []
        self.active = {}

    def visit(self, record: Record):
        packet = record.packet
        try:
            base = ipv4_offset(packet, record.linktype)
            if len(packet) < base + 20 or packet[base] >> 4 != 4:
                return None
            ihl = (packet[base] & 15) * 4
            if ihl < 20 or base + ihl > len(packet):
                return None
            proto = packet[base + 9]
            if proto not in (6, 17):
                return None
            # Treat non-first fragments as unsupported to avoid misreading data as ports.
            if struct.unpack_from('!H', packet, base + 6)[0] & 0x3fff:
                return None
            l4 = base + ihl
            if len(packet) < l4 + (20 if proto == 6 else 8):
                return None
            a = ('.'.join(str(x) for x in packet[base+12:base+16]),
                 struct.unpack_from('!H', packet, l4)[0])
            b = ('.'.join(str(x) for x in packet[base+16:base+20]),
                 struct.unpack_from('!H', packet, l4+2)[0])
            key = (proto, tuple(sorted((a,b))))
            uid = self.active.get(key)
            if uid is None or record.time - self.flows[uid].last > self.idle_timeout:
                uid = len(self.flows)
                self.flows.append(FlowSummary(uid, 'TCP' if proto == 6 else 'UDP', a, b,
                                              record.time, record.time, first_packet_index=record.index))
                self.active[key] = uid
            flow = self.flows[uid]
            flow.last = record.time
            flow.packet_count += 1
            flow.length_sum += struct.unpack_from('!H', packet, base+2)[0]
            flow.ttl_sum += packet[base+8]
            flow.forward_sum += int(a == flow.endpoint_a)
            return uid
        except (CaptureError, struct.error, TypeError, AttributeError):
            return None


def _write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name('.' + path.name + '.tmp')
    with temp.open('w', encoding='utf-8') as handle:
        json.dump(data, handle, indent=2, allow_nan=False)
        handle.write('\n')
    os.replace(temp, path)


def scan_capture(path: Path, idle_timeout: float, *, progress=None):
    tracker = FlowTracker(idle_timeout)
    total, unsupported = 0, 0
    last_unsupported = -1
    for rec in iter_records(path):
        if rec.packet is None:
            continue
        uid = tracker.visit(rec)
        if uid is None:
            unsupported += 1
            last_unsupported = rec.index
        total += 1
        if progress and total % 100_000 == 0:
            progress('scanning', total, None)
    if not total or not tracker.flows:
        raise ValueError('No supported IPv4 TCP/UDP flows found')
    return tracker.flows, total, unsupported, last_unsupported


def extract_capture(input_path, feature_path, *, idle_timeout=60.0, max_packets=None):
    """Extract flow features in O(number of flows) RAM, never store packets."""
    if max_packets is not None:
        raise ValueError('Bounded-memory extraction supports complete captures only; make a separate pilot capture')
    flows, total, unsupported, _ = scan_capture(input_path, idle_timeout)
    feature_path.parent.mkdir(parents=True, exist_ok=True)
    with feature_path.open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(f.features() for f in flows)
    desc = {'input': str(input_path), 'input_sha256': sha256(input_path),
            'packets_used': total, 'flow_count': len(flows),
            'unsupported_packet_count': unsupported, 'max_packets': None,
            'idle_timeout': idle_timeout, 'columns': COLUMNS}
    _write_json(feature_path.with_suffix('.metadata.json'), desc)
    print(json.dumps(desc, indent=2), flush=True)


def _level_starts(levels, total, basis, sizes):
    if basis == 'packets':
        return {p: total - (max(1, math.floor(total*p/100+.5)) if p not in (0,100)
                            else total if p == 100 else 0) for p in levels}
    # Packet-byte selection from the tail without loading individual packets.
    total_bytes = int(sizes[:, 3].sum())
    suffix_bytes, starts = 0, {0: total, 100: 0}
    pending = sorted((p for p in levels if 0 < p < 100))
    # Find nearest suffix to each target (deterministic ties).
    for idx in range(total - 1, -1, -1):
        if not pending:
            break
        prev = suffix_bytes
        suffix_bytes += int(sizes[idx, 3])
        while pending and suffix_bytes >= total_bytes*pending[0]/100:
            p = pending.pop(0)
            target = total_bytes*p/100
            starts[p] = idx if abs(suffix_bytes-target) <= abs(prev-target) else idx+1
    return starts


def _sum_clip(mm, start, end, offset, *, chunk=65_536):
    import numpy as np
    total = 0
    for i in range(start, end, chunk):
        block = np.asarray(mm[i:min(i+chunk, end), :3])
        total += int(np.clip(block[:,0].astype('int64') + offset, block[:,1], block[:,2]).sum())
    return total


def _balance_band(mm, first, end):
    """Constant-RAM, disk-backed version of the original additive byte balancing."""
    import numpy as np
    if end <= first:
        return
    target = int(mm[first:end,3].sum())
    feasible_lo = int(mm[first:end,1].sum())
    feasible_hi = int(mm[first:end,2].sum())
    target = min(feasible_hi, max(feasible_lo, target))
    low, high = -65536, 65536
    # Find additive offset for the model's preferred lengths, subject to bounds.
    while low < high:
        mid = (low+high)//2
        if _sum_clip(mm, first, end, mid) < target:
            low = mid+1
        else:
            high = mid
    for i in range(first,end,65_536):
        stop = min(end,i+65_536)
        block = np.asarray(mm[i:stop,:3])
        mm[i:stop,4] = np.clip(block[:,0].astype('int64')+low,block[:,1],block[:,2])
    mm.flush()
    delta = target - int(mm[first:end,4].sum())
    # Binary search rounds to an integer length. Correct small residual exactly.
    if delta:
        direction = 1 if delta > 0 else -1
        while delta:
            before = delta
            for i in range(first,end,65_536):
                if not delta:
                    break
                stop = min(end,i+65_536)
                block = np.asarray(mm[i:stop,4])
                bounds = np.asarray(mm[i:stop,2 if direction>0 else 1])
                available = np.flatnonzero(block < bounds if direction > 0 else block > bounds)
                count = min(len(available),abs(delta))
                if count:
                    chosen = available[:count]
                    mm[i+chosen,4] += direction
                    delta -= direction*count
            if before == delta:
                break
    if delta:
        raise AssertionError(f'Could not balance incremental suffix packet lengths: {delta}')
    mm.flush()


@contextmanager
def _packet_plan(total: int):
    """Use local scratch rather than the results mount, closing mmap before cleanup.

    Remote filesystems may fail to remove an open memory-mapped plan with
    ENOTEMPTY. The scratch directory can be overridden with
    CBR_GAN_SCRATCH_DIR (e.g. a large local NVMe mount).
    """
    import numpy as np
    scratch = os.environ.get('CBR_GAN_SCRATCH_DIR') or None
    with tempfile.TemporaryDirectory(prefix='gan-length-plan-', dir=scratch) as planning:
        free = shutil.disk_usage(planning).free
        needed = total * 6 * 4
        if free < needed + max(64 * 1024 * 1024, needed // 20):
            raise OSError(f'Insufficient GAN scratch disk space: need ~{needed:,} bytes '
                          f'plus reserve, have {free:,} bytes in {planning}. '
                          'Set CBR_GAN_SCRATCH_DIR to a large local scratch directory.')
        mm = np.memmap(Path(planning) / 'plan.dat', mode='w+',
                       dtype='int32', shape=(total, 6))
        try:
            yield mm
        finally:
            # Critical on CIFS/NFS mounts: unlink cannot safely clean up a
            # mapped file while its mapping/OS handle remains open.
            try:
                mm.flush()
            finally:
                mm._mmap.close()


def stream_sweep(input_path, model_path, output_dir, percentages, *,
                 sweep_percentages=None, seed=42, idle_timeout=60.0,
                 basis='packets', max_ip_length=1500, max_packets=None):
    """Reproducible bounded-memory suffix sweep, returns list of result records.

    sweep_percentages anchors incremental byte-balancing bands even when only
    one requested output is generated/evaluated per subprocess invocation.
    """
    if max_packets is not None:
        raise ValueError('Use a separate pilot capture; truncating changes the experiment basis')
    import numpy as np
    import torch
    input_path, model_path, output_dir = map(Path,(input_path,model_path,output_dir))
    requested = sorted(set(int(p) for p in percentages))
    full_levels = sorted(set(int(p) for p in (sweep_percentages or requested)))
    if not requested or any(p < 0 or p > 100 for p in full_levels) or not set(requested) <= set(full_levels):
        raise ValueError('Requested levels must be a subset of valid sweep levels')
    if basis not in ('packets','packet-bytes'):
        raise ValueError('Unknown packet percentage basis')
    output_dir.mkdir(parents=True,exist_ok=True)
    checkpoint = output_dir / 'generation_progress.json'
    start_wall = time.monotonic()
    def update(stage, done=0, total=None, pct=None):
        if done and done % 100_000 and total is None:
            return
        elapsed = max(1e-6,time.monotonic()-start_wall)
        _write_json(checkpoint, {'stage':stage,'packets_done':done,'total_packets':total,
                                 'percentage':pct,'updated_at':time.time(),
                                 'elapsed_seconds':elapsed,
                                 'packets_per_second':round(done/elapsed,2) if done else None})
        print(f'GAN stage={stage} level={pct} packets={done:,}/{total if total is not None else "?"}', flush=True)
    update('scanning')
    flows,total,unsupported,last_unsupported = scan_capture(input_path,idle_timeout,progress=update)
    np.random.seed(seed)
    torch.manual_seed(seed)
    with model_path.open('rb') as handle:
        model = pickle.load(handle)['ctgan']  # trusted local training artifact only
    features = {}
    for protocol in sorted({f.protocol for f in flows}):
        group = [f for f in flows if f.protocol == protocol]
        table = model.sample(len(group), condition_column='protocol',condition_value=protocol)
        if not set(COLUMNS) <= set(table.columns) or len(table) != len(group):
            raise ValueError('GAN model output has inconsistent feature columns')
        for flow, (_,row) in zip(group,table.iterrows()):
            features[flow.uid] = row.to_dict()
    # A disk-backed packet plan takes ~24 bytes per source packet rather than
    # retaining input packets, source capture bytes, and 11 rebuilt captures.
    with _packet_plan(total) as mm:
        tracker = FlowTracker(idle_timeout)
        packet_index = 0
        update('planning',0,total)
        for rec in iter_records(input_path):
            if rec.packet is None:
                continue
            uid = tracker.visit(rec)
            if uid is not None:
                pref,lo,hi = propose_length(
                    rec.packet,rec.linktype,features[uid],
                    flows[uid].length_sum / flows[uid].packet_count,
                    seed*1000003+packet_index,max_ip_length)
            else:
                pref = lo = hi = len(rec.packet)
            mm[packet_index] = (pref,lo,hi,len(rec.packet),len(rec.packet),uid if uid is not None else -1)
            packet_index += 1
            if packet_index % 100_000 == 0:
                update('planning',packet_index,total)
        mm.flush()
        if packet_index != total:
            raise AssertionError('Packet plan count changed between passes')
        starts = _level_starts(full_levels,total,basis,mm)
        earliest = min(starts[p] for p in requested)
        if last_unsupported >= earliest:
            raise ValueError(f'Packet {last_unsupported} cannot be replaced (non-IPv4 TCP/UDP or invalid header). Filter the capture first.')
        boundaries = sorted(set(starts.values())|{total},reverse=True)
        update('balancing',0,total)
        for right,left in zip(boundaries[:-1],boundaries[1:]):
            _balance_band(mm,left,right)
        update('writing',0,total)
        input_hash, model_hash = sha256(input_path), sha256(model_path)
        results = []
        for level in requested:
            suffix = starts[level]
            output = output_dir/f'gan_{level:03d}{input_path.suffix.lower()}'
            pkt_labels = output_dir/f'gan_{level:03d}.packet_labels.csv'
            flow_labels = output_dir/f'gan_{level:03d}.flow_labels.csv'
            temp_output = output.with_name('.'+output.name+'.partial')
            temp_labels = pkt_labels.with_name('.'+pkt_labels.name+'.partial')
            num_replaced = total-suffix
            try:
                if level==0:
                    shutil.copyfile(input_path,temp_output)
                # Write provenance without accumulating rows or re-reading
                # the output; byte-identical original records at level 0.
                counts = [0] * len(flows)
                with temp_labels.open('w',newline='',encoding='utf-8') as handle:
                    writer = csv.writer(handle)
                    writer.writerow(['output_packet_index','origin','source_flow_uid',
                                     'original_packet_bytes','output_packet_bytes'])
                    with (temp_output.open('wb') if level else open(os.devnull,'wb')) as output_stream:
                        for rec in iter_records(input_path):
                            if rec.packet is None:
                                if level:
                                    output_stream.write(rec.raw)
                                continue
                            idx = rec.index
                            uid = int(mm[idx,5])
                            uid = None if uid < 0 else uid
                            changed = idx>=suffix
                            if changed and uid is not None:
                                counts[uid] += 1
                            if changed and uid is None:
                                raise ValueError(f'Unsupported packet at index {idx}')
                            if level:
                                updated = (synthesize_variable_length(
                                    rec.packet,rec.linktype,features[uid],
                                    seed*1000003+idx,int(mm[idx,4]),
                                    flows[uid].endpoint_b[1]) if changed else None)
                                output_stream.write(rec.rewrite(updated) if changed else rec.raw)
                            writer.writerow([idx,'synthetic' if changed else 'real',
                                             uid,len(rec.packet),int(mm[idx,4]) if changed else len(rec.packet)])
                            if (idx+1)%100_000==0:
                                update('writing',idx+1,total,level)
                os.replace(temp_output,output)
                os.replace(temp_labels,pkt_labels)
            finally:
                temp_output.unlink(missing_ok=True)
                temp_labels.unlink(missing_ok=True)
            with flow_labels.open('w',newline='',encoding='utf-8') as stream:
                writer = csv.writer(stream)
                writer.writerow(['source_flow_uid','packet_count','synthetic_packet_count',
                                 'first_packet_index','flow_start_time','original_protocol'])
                for flow in flows:
                    writer.writerow([flow.uid,flow.packet_count,counts[flow.uid],
                                     flow.first_packet_index,flow.first,flow.protocol])
            actual_size = output.stat().st_size
            source_size = input_path.stat().st_size
            generated_bytes = int(mm[suffix:total,4].sum())
            source_bytes = int(mm[:,3].sum())
            changed_bytes = int(mm[suffix:total,3].sum())
            entry = {
                'requested_pct':level,'percentage_basis':basis,
                'actual_packet_pct':round(100*num_replaced/total,6),
                'actual_replaced_source_packet_byte_pct':round(100*changed_bytes/source_bytes,6),
                'synthetic_packets':num_replaced,'real_packets':suffix,'total_packets':total,
                'source_packet_data_bytes':source_bytes,
                'output_packet_data_bytes':int(mm[:suffix,3].sum())+generated_bytes,
                'source_file_bytes':source_size,'output_file_bytes':actual_size,
                'file_size_error_pct':round(100*(actual_size-source_size)/source_size,6),
                'file_size_equal':actual_size==source_size,
                'untouched_prefix_packet_count':suffix,
                'flow_count':len(flows),
                'flows_touched':sum(1 for n in counts if n),
                'flows_fully_replaced':sum(int(counts[f.uid]==f.packet_count) for f in flows),
                'flows_partially_replaced':sum(int(0<counts[f.uid]<f.packet_count) for f in flows),
                'output':output.name,'packet_labels':pkt_labels.name,
                'flow_labels':flow_labels.name,'input_sha256':input_hash,
                'output_sha256':sha256(output),'model_sha256':model_hash,
                'seed':seed,
                'note':'bounded-memory CTGAN-conditioned suffix; byte-balanced bands; '
                       'source timestamps unchanged; no application-level protocol coherence',
            }
            _write_json(output_dir/f'gan_{level:03d}.json',entry)
            results.append(entry)
            print(f'GAN-{level:03d}: replaced {num_replaced:,}/{total:,} packets '
                  f'({entry["actual_packet_pct"]:.3f}%), size error '
                  f'{entry["file_size_error_pct"]:+.4f}%, output {output}', flush=True)
        with (output_dir/'manifest.csv').open('w',newline='',encoding='utf-8') as stream:
            writer = csv.DictWriter(stream,fieldnames=list(results[0]))
            writer.writeheader()
            writer.writerows(results)
        update('completed',total,total,requested[-1])
        return results


def cli(argv=None):
    """Compatible extract/sweep CLI for the existing traffic_gan.py wrapper."""
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    for command in ('extract','sweep'):
        args = sub.add_parser(command)
        args.add_argument('--input',type=Path,required=True)
        args.add_argument('--idle-timeout',type=float,default=60)
        args.add_argument('--max-packets',type=int,default=None)
        if command == 'extract':
            args.add_argument('--features',type=Path,required=True)
        else:
            args.add_argument('--model',type=Path,required=True)
            args.add_argument('--output',type=Path,required=True)
            args.add_argument('--percentages',default='0,10,20,30,40,50,60,70,80,90,100')
            args.add_argument('--sweep-percentages',default=None,
                              help='Complete original percentage grid for deterministic byte balancing')
            args.add_argument('--seed',type=int,default=42)
            args.add_argument('--basis',choices=('packets','packet-bytes'),default='packets')
            args.add_argument('--max-ip-length',type=int,default=1500)
    args = parser.parse_args(argv)
    if args.command == 'extract':
        extract_capture(args.input,args.features,idle_timeout=args.idle_timeout,
                        max_packets=args.max_packets)
    else:
        convert = lambda raw: [int(s.strip()) for s in raw.split(',') if s.strip()]
        stream_sweep(args.input,args.model,args.output,convert(args.percentages),
                     sweep_percentages=convert(args.sweep_percentages) if args.sweep_percentages else None,
                     seed=args.seed,idle_timeout=args.idle_timeout,basis=args.basis,
                     max_ip_length=args.max_ip_length,max_packets=args.max_packets)


if __name__ == '__main__':
    cli()