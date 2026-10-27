"""Small PCAP fixtures for new opt-in protocol diagnostics and temporal GAN."""
from pathlib import Path

import pytest

scapy = pytest.importorskip("scapy")
from scapy.all import Ether, IP, TCP, UDP, Raw, PcapWriter, PcapReader

from cbr_tests.metrics.pcap_integrity_v2 import (
    run_exact_packet_duplicate_ratio, run_packet_integrity_profile,
    run_tcp_sequence_continuity_profile,
)


def make_trace(path: Path, count: int = 10, start: float = 100.0):
    with PcapWriter(str(path), sync=True) as out:
        for i in range(count):
            p = Ether()/IP(src="192.0.2.10", dst="198.51.100.10")/UDP(sport=30000, dport=443)/Raw(bytes([i%251])*20)
            p.time = start + i*0.01
            out.write(p)


def test_exact_duplicates_are_not_field_signature_repetition(tmp_path):
    p = tmp_path/"capture.pcap"
    make_trace(p, count=4)
    ok, result = run_exact_packet_duplicate_ratio(p, {"calculation":{"parameters":{"max_packets":20}}})
    assert ok
    assert result["test_results"]["exact_packet_duplicate_ratio"]["duplicate_packet_count"] == 0


def test_duplicate_same_bytes_and_timestamp_detected(tmp_path):
    p = tmp_path/"repeated.pcap"
    frame = Ether()/IP(src="192.0.2.1", dst="198.51.100.1")/UDP(sport=20000, dport=443)/Raw(b"x")
    frame.time = 1000.
    with PcapWriter(str(p), sync=True) as writer:
        writer.write(frame)
        writer.write(frame)
    ok, result = run_exact_packet_duplicate_ratio(p, {"calculation":{"parameters":{"max_packets":20}}})
    assert ok
    assert result["test_results"]["exact_packet_duplicate_ratio"]["duplicate_packet_count"] == 1


def test_ipv4_length_mismatch_reported_without_claiming_failure(tmp_path):
    p = tmp_path/"invalid.pcap"
    pkt = Ether()/IP(src="192.0.2.10", dst="198.51.100.10", len=20)/UDP(sport=1,dport=443)/Raw(b"payload")
    pkt.time = 1.
    with PcapWriter(str(p), sync=True) as writer:
        writer.write(pkt)
    ok, result = run_packet_integrity_profile(p, {"calculation":{"parameters":{"max_packets":20}}})
    assert ok
    v = result["test_results"]["packet_integrity_profile"]
    assert v["length_inconsistency_count"] >= 1
    assert v["status"] == "informational"


def test_tcp_sequence_gaps_are_evidence_not_realism_verdict(tmp_path):
    p = tmp_path/"tcp.pcap"
    with PcapWriter(str(p), sync=True) as writer:
        for i, sequence in enumerate([100, 110, 500]):
            pkt = Ether()/IP(src="192.0.2.10", dst="198.51.100.10")/TCP(sport=51515,dport=443,flags="PA",seq=sequence)/Raw(b"0123456789")
            pkt.time = 10.0 + i*0.1
            writer.write(pkt)
    ok, result = run_tcp_sequence_continuity_profile(p, {"calculation":{"parameters":{"max_packets":20}}})
    assert ok
    v = result["test_results"]["tcp_sequence_continuity_profile"]
    assert v["forward_gap_count"] == 1
    assert v["status"] == "informational"


@pytest.mark.slow
def test_temporal_gan_small_end_to_end(tmp_path):
    torch = pytest.importorskip("torch")
    from gan_generator.temporal_gan import train, generate, file_hash
    training = tmp_path/"train.pcap"
    heldout = tmp_path/"heldout.pcap"
    make_trace(training, count=256, start=10.)
    make_trace(heldout, count=256, start=100.)
    model = tmp_path/"model.pt"
    details = train([training], model, epochs=1, seed=13, sequence_length=8,
                    device="cpu", max_packets=256, batch_size=16)
    assert model.exists()
    assert file_hash(heldout) not in details["training_pcap_sha256"].values()
    generated = tmp_path/"timing_050.pcap"
    metadata = generate(heldout, model, generated, percentage=50,
                        seed=14, pattern="distributed", duration_policy="match_original")
    assert generated.is_file()
    assert metadata["packet_count"] == 256
    assert metadata["replaced_gap_count"] == round(255*.5)
    assert metadata["original_duration_seconds"] == pytest.approx(
        metadata["output_duration_seconds"], abs=1e-5)
    with PcapReader(str(heldout)) as a, PcapReader(str(generated)) as b:
        old, new = list(a), list(b)
    assert [bytes(p) for p in old] == [bytes(p) for p in new]
    assert all(float(new[i+1].time) >= float(new[i].time) for i in range(len(new)-1))
    with pytest.raises(ValueError, match="Training leakage"):
        generate(training, model, tmp_path/"bad.pcap", percentage=50)
