"""End-to-end streaming GAN regression checks with tiny synthetic PCAPs."""
from __future__ import annotations

import hashlib
import importlib
import json
import pickle
import struct
import sys
import tarfile
from pathlib import Path

import pytest


class FakeGAN:
    """Pickleable generator with a consistent per-protocol feature distribution."""
    def sample(self, n, condition_column=None, condition_value=None):
        pd = pytest.importorskip('pandas')
        return pd.DataFrame([{
            'protocol': condition_value, 'mean_ttl': 90, 'service_port': '8443',
            'mean_ip_length': 180, 'packet_count_log': 3.0,
            'duration_ms_log': 5.0, 'forward_fraction': 0.5,
        } for _ in range(n)])


@pytest.fixture
def tools(tmp_path, monkeypatch):
    pytest.importorskip('numpy')
    pytest.importorskip('torch')
    root = Path(__file__).resolve().parents[1]
    archive = root / 'gan_generator' / 'ctgan-pcap-same-count-source.tar.xz'
    extracted = tmp_path / 'legacy_source'
    extracted.mkdir()
    with tarfile.open(archive, 'r:xz') as handle:
        for name in ('tail_replacement.py', 'variable_replacement.py'):
            source = handle.extractfile(f'ctgan-pcap-same-count/{name}')
            assert source is not None
            (extracted / name).write_bytes(source.read())
    monkeypatch.syspath_prepend(str(extracted))
    monkeypatch.syspath_prepend(str(root / 'gan_generator'))
    for name in ('streaming_sweep','tail_replacement','variable_replacement'):
        sys.modules.pop(name, None)
    streaming = importlib.import_module('streaming_sweep')
    legacy = importlib.import_module('tail_replacement')
    return streaming, legacy


def checksum(buf):
    if len(buf) % 2: buf += b'\0'
    words = struct.unpack('!' + 'H'*(len(buf)//2),buf)
    value = sum(words)
    while value>>16: value=(value&0xffff)+(value>>16)
    return (~value)&0xffff


def packet(idx, udp=False):
    payload=f'original payload {idx:04d}'.encode()
    src,dst=b'\x0a\x00\x00\x01', b'\x0a\x00\x00\x02'
    proto=17 if udp else 6
    if udp:
        segment=bytearray(struct.pack('!HHHH',50000,53,8+len(payload),0)+payload)
        cksum_offset=6
    else:
        segment=bytearray(struct.pack('!HHIIHHHH',50000,443,100+idx,200,(5<<12)|0x18,8192,0,0)+payload)
        cksum_offset=16
    pseudo=src+dst+struct.pack('!BBH',0,proto,len(segment))
    struct.pack_into('!H',segment,cksum_offset,checksum(pseudo+segment))
    ip=bytearray(struct.pack('!BBHHHBBH4s4s',0x45,0,20+len(segment),idx,0,64,proto,0,src,dst))
    struct.pack_into('!H',ip,10,checksum(ip))
    return b'\x00'*6+b'\x11'*6+struct.pack('!H',0x0800)+ip+segment


def _block(kind,body):
    body += b'\0'*(-len(body)%4)
    size=len(body)+12
    return struct.pack('<II',kind,size)+body+struct.pack('<I',size)


def capture(packets,fmt):
    if fmt=='pcap':
        out=bytearray(struct.pack('<IHHIIII',0xa1b2c3d4,2,4,0,0,65535,1))
        for i,pkt in enumerate(packets):
            out+=struct.pack('<IIII',100_000+i,456,len(pkt),len(pkt))+pkt
        return bytes(out)
    header=_block(0x0a0d0d0a,struct.pack('<IHHq',0x1a2b3c4d,1,0,-1))
    idb=_block(1,struct.pack('<HHI',1,0,65535))
    return header+idb+b''.join(_block(6,struct.pack('<IIIII',0,0,1000+i,len(pkt),len(pkt))+pkt)
                              for i,pkt in enumerate(packets))


@pytest.mark.parametrize('fmt',['pcap','pcapng'])
def test_streaming_preserves_counts_size_checksums_nestedness_and_baseline(tmp_path,tools,fmt):
    gan,legacy=tools
    source=capture([packet(i,udp=bool(i%2)) for i in range(120)],fmt)
    inp=tmp_path/f'in.{fmt}'
    inp.write_bytes(source)
    flows, count, unsupported, _=gan.scan_capture(inp,60)
    assert count==120 and unsupported==0
    assert sum(f.packet_count for f in flows)==120
    features=tmp_path/'flows.csv'
    gan.extract_capture(inp,features)
    assert 'packet_count_log' in features.read_text()
    assert json.loads(features.with_suffix('.metadata.json').read_text())['packets_used']==120
    model=tmp_path/'model.pkl'
    with model.open('wb') as output: pickle.dump({'ctgan':FakeGAN()},output)
    output_dir=tmp_path/'outputs'
    results=gan.stream_sweep(inp,model,output_dir,[0,10,20,100],seed=42)
    assert len(results)==4
    original_slots=legacy.locate_packet_slots(source)
    for pct in (0,10,20,100):
        file=output_dir/f'gan_{pct:03d}.{fmt}'
        generated=file.read_bytes()
        slots=legacy.locate_packet_slots(generated)
        assert len(slots)==120
        assert sum(s.length for s in original_slots)==sum(s.length for s in slots)
        assert abs(len(generated)-len(source))<=120
        record=json.loads((output_dir/f'gan_{pct:03d}.json').read_text())
        assert record['synthetic_packets']==int(120*pct/100)
        assert record['output_file_bytes']==len(generated)
        assert record['output_sha256']==hashlib.sha256(generated).hexdigest()
        assert generated==source if pct==0 else generated!=source
        if pct:
            for slot in slots[-int(120*pct/100):]:
                raw=generated[slot.offset:slot.offset+slot.length]
                ip=raw[14:]
                assert checksum(ip[:20])==0
                proto=ip[9]
                payload=ip[20:]
                assert checksum(ip[12:20]+struct.pack('!BBH',0,proto,len(payload))+payload)==0
    ten=(output_dir/f'gan_010.{fmt}').read_bytes()
    twenty=(output_dir/f'gan_020.{fmt}').read_bytes()
    a=legacy.locate_packet_slots(ten)[-1]
    b=legacy.locate_packet_slots(twenty)[-1]
    assert ten[a.offset:a.offset+a.length]==twenty[b.offset:b.offset+b.length]
    # Generating one level at a time uses the complete sweep's byte bands
    # and produces the exact same deterministic output as the full batch.
    lone=tmp_path/'single'
    gan.stream_sweep(inp,model,lone,[10],sweep_percentages=[0,10,20,100],seed=42)
    assert (lone/f'gan_010.{fmt}').read_bytes()==ten
    assert list(output_dir.glob('gan_*.json'))
    assert (output_dir/'generation_progress.json').is_file()


def test_protected_packet_rejected_before_writing_output(tmp_path,tools):
    gan,legacy=tools
    raw=bytearray(capture([packet(i) for i in range(20)],'pcap'))
    last=legacy.locate_packet_slots(bytes(raw))[-1]
    raw[last.offset+12:last.offset+14]=b'\x86\xdd'  # IPv6 EtherType
    inp=tmp_path/'unusable.pcap';inp.write_bytes(raw)
    model=tmp_path/'model.pkl'
    with model.open('wb') as stream:pickle.dump({'ctgan':FakeGAN()},stream)
    target=tmp_path/'target'
    with pytest.raises(ValueError,match='cannot be replaced'):
        gan.stream_sweep(inp,model,target,[100],sweep_percentages=[0,100])
    assert not list(target.glob('gan_*.pcap'))


def test_packet_plan_closes_memmap_before_deleting_scratch(tmp_path, tools, monkeypatch):
    import numpy as np
    gan, _ = tools
    mappings = []
    original = np.memmap

    def tracking_memmap(*args, **kwargs):
        mapping = original(*args, **kwargs)
        mappings.append(mapping)
        return mapping

    monkeypatch.setattr(np, 'memmap', tracking_memmap)
    monkeypatch.setenv('CBR_GAN_SCRATCH_DIR', str(tmp_path))
    with gan._packet_plan(100) as mm:
        mm[:, 0] = 17
        assert mm._mmap.closed is False
    assert mappings and all(m._mmap.closed for m in mappings)
    assert not list(tmp_path.glob('gan-length-plan-*'))


def test_invalid_capture_reports_packet_number_offset_and_lengths(tmp_path, tools):
    gan, _ = tools
    source = bytearray(capture([packet(0), packet(1)], 'pcap'))
    struct.pack_into('<II', source, 24 + 8, 400, 20)
    bad = tmp_path / 'bad.pcap'
    bad.write_bytes(source)
    with pytest.raises(gan.CaptureError, match=r'record 0.*offset 24.*captured_length=400.*original_length=20'):
        gan.scan_capture(bad, 60)


def test_zero_length_pcap_record_is_preserved_and_unsupported(tmp_path, tools):
    gan, _ = tools
    source = capture([packet(0), packet(1)], 'pcap')
    first_end = 24 + 16 + len(packet(0))
    modified = source[:first_end] + struct.pack('<IIII', 100001, 0, 0, 0) + source[first_end:]
    file = tmp_path / 'zero.pcap'
    file.write_bytes(modified)
    flows, total, unsupported, last = gan.scan_capture(file, 60)
    assert total == 3
    assert unsupported == 1
    assert last == 1
    assert sum(f.packet_count for f in flows) == 2
