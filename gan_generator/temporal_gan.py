"""Opt-in sequential temporal GAN for packet inter-arrival times.

Distinct from the CTGAN packet-template generator. Learns timing on independent
PCAPs; produces timestamp-only PCAP variants preserving original packet bytes,
order and packet count. It does not generate valid end-to-end conversations.

Usage:
    python -m gan_generator.temporal_gan train --inputs train1.pcap train2.pcap --model temporal.pt
    python -m gan_generator.temporal_gan sweep --input heldout.pcap --model temporal.pt --out sweep
Only locally trained/trusted weight checkpoints may be loaded. Never pass an
untrusted pickle file as a model. The full sweep records seed and hashes.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import random

import numpy as np


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for part in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(part)
    return digest.hexdigest()


def _deps():
    import torch
    from scapy.utils import PcapReader, PcapWriter
    return torch, PcapReader, PcapWriter


def _seed_all(seed: int):
    torch, _, _ = _deps()
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def _observe(path: Path, max_packets: int | None = 300_000) -> tuple[np.ndarray, np.ndarray]:
    """Packet gaps and log packet length conditions in capture order."""
    _, Reader, _ = _deps()
    times: list[float] = []
    lengths: list[int] = []
    with Reader(str(path)) as capture:
        for packet in capture:
            times.append(float(packet.time))
            lengths.append(len(packet))
            if max_packets is not None and len(times) >= max_packets:
                break
    if len(times) < 3:
        raise ValueError(f"Not enough packets to learn timings in {path}")
    times_a = np.asarray(times, dtype=np.float64)
    gaps = np.maximum(np.diff(times_a), 0.0)
    # 1 ms granularity for a numerically stable learning target; preserve
    # sub-ms detail through the original captured packet times, if desired.
    targets = np.log1p(gaps * 1000.0)
    conditions = np.log1p(np.asarray(lengths[1:], dtype=np.float64))
    return targets, conditions


def _windows(targets: np.ndarray, conditions: np.ndarray, sequence_length: int,
             max_windows: int = 30000) -> np.ndarray:
    if len(targets) < sequence_length:
        raise ValueError("Training sequence shorter than sequence_length")
    positions = np.arange(0, len(targets) - sequence_length + 1, sequence_length)
    if len(positions) > max_windows:
        positions = positions[np.linspace(0, len(positions)-1, max_windows, dtype=int)]
    rows = np.stack([np.stack((targets[i:i+sequence_length],
                               conditions[i:i+sequence_length]), axis=-1) for i in positions])
    return rows.astype(np.float32)


def build_models(sequence_length: int, hidden_size: int = 64):
    torch, _, _ = _deps()
    from torch import nn

    class TimingGenerator(nn.Module):
        def __init__(self):
            super().__init__()
            self.rnn = nn.GRU(3, hidden_size, batch_first=True)
            self.head = nn.Sequential(nn.Linear(hidden_size, hidden_size), nn.ReLU(),
                                      nn.Linear(hidden_size, 1), nn.Softplus())
        def forward(self, noise, condition):
            data = torch.cat([noise, condition], dim=-1)
            hidden, _ = self.rnn(data)
            return self.head(hidden)

    class TimingDiscriminator(nn.Module):
        def __init__(self):
            super().__init__()
            self.rnn = nn.GRU(2, hidden_size, batch_first=True)
            self.head = nn.Linear(hidden_size, 1)
        def forward(self, gaps, condition):
            hidden, _ = self.rnn(torch.cat([gaps, condition], dim=-1))
            return self.head(hidden[:, -1])

    return TimingGenerator(), TimingDiscriminator()


def train(inputs: list[Path], model: Path, epochs: int = 20, seed: int = 42,
          sequence_length: int = 32, device: str = "cpu", max_packets: int = 300_000,
          batch_size: int = 64) -> dict:
    torch, _, _ = _deps()
    from torch import nn
    from torch.utils.data import DataLoader, TensorDataset
    if not inputs or epochs < 1 or sequence_length < 4:
        raise ValueError("At least one training PCAP, positive epochs and sequence_length >= 4 required")
    if device.startswith("cuda") and not torch.cuda.is_available():
        raise RuntimeError("CUDA requested but unavailable")
    _seed_all(seed)
    observed = [_observe(p, max_packets=max_packets) for p in inputs]
    raw = np.concatenate([x[0] for x in observed])
    cond = np.concatenate([x[1] for x in observed])
    # Normalise once from training data, never from held-out captures.
    scale = float(max(np.std(raw), 1e-4))
    offset = float(np.mean(raw))
    cscale = float(max(np.std(cond), 1e-4))
    coffset = float(np.mean(cond))
    windows = np.concatenate([_windows((x[0]-offset)/scale, (x[1]-coffset)/cscale,
                                        sequence_length) for x in observed])
    if len(windows) < 4:
        raise ValueError("Insufficient independent training windows")
    dl = DataLoader(TensorDataset(torch.from_numpy(windows)), batch_size=batch_size, shuffle=True)
    generator, discriminator = build_models(sequence_length)
    generator, discriminator = generator.to(device), discriminator.to(device)
    optimizer_g = torch.optim.Adam(generator.parameters(), lr=2e-4, betas=(0.5, 0.9))
    optimizer_d = torch.optim.Adam(discriminator.parameters(), lr=2e-4, betas=(0.5, 0.9))
    bce = nn.BCEWithLogitsLoss()
    history = []
    for epoch in range(epochs):
        gen_loss = dis_loss = 0.0
        for (batch,) in dl:
            batch = batch.to(device)
            real_gap, condition = batch[:, :, :1], batch[:, :, 1:]
            # Softplus target requires nonnegative output. Train in physical
            # log1p milliseconds and invert below after synthesis.
            physical = torch.clamp(real_gap * scale + offset, min=0)
            z = torch.randn(batch.shape[0], sequence_length, 2, device=device)
            fake = generator(z, condition)
            optimizer_d.zero_grad()
            loss_d = 0.5 * (bce(discriminator(physical, condition),
                                torch.ones(batch.shape[0], 1, device=device))
                        + bce(discriminator(fake.detach(), condition),
                                torch.zeros(batch.shape[0], 1, device=device)))
            loss_d.backward()
            optimizer_d.step()
            optimizer_g.zero_grad()
            z = torch.randn(batch.shape[0], sequence_length, 2, device=device)
            fake = generator(z, condition)
            # Small distribution anchoring term prevents pure collapse but does
            # not guarantee realism; evaluate with independent metric suite.
            loss_g = bce(discriminator(fake, condition),
                         torch.ones(batch.shape[0], 1, device=device)) + 0.1 * torch.abs(fake.mean()-physical.mean())
            loss_g.backward()
            optimizer_g.step()
            gen_loss += float(loss_g.item())
            dis_loss += float(loss_d.item())
        history.append({"epoch": epoch+1, "generator_loss": gen_loss/len(dl),
                        "discriminator_loss": dis_loss/len(dl)})
    model.parent.mkdir(parents=True, exist_ok=True)
    torch.save({k: v.cpu() for k, v in generator.state_dict().items()}, str(model))
    meta = {"format": "temporal-gan-v1", "seed": seed, "training_pcap_sha256": {
            str(p.resolve()): file_hash(p) for p in inputs}, "sequence_length": sequence_length,
            "hidden_size": 64, "epochs": epochs, "max_training_packets_per_capture": max_packets,
            "target_unit": "log1p_milliseconds", "condition": "log1p_packet_length",
            "training_mean": offset, "training_std": scale, "condition_mean": coffset,
            "condition_std": cscale, "history": history,
            "created_at_utc": datetime.now(timezone.utc).isoformat()}
    model.with_suffix(model.suffix+".json").write_text(json.dumps(meta, indent=2)+"\n")
    return meta


def _generated_gaps(input_path: Path, model_path: Path, seed: int, device: str) -> np.ndarray:
    torch, _, _ = _deps()
    if device.startswith("cuda") and not torch.cuda.is_available():
        raise RuntimeError("CUDA requested but unavailable")
    meta = json.loads(model_path.with_suffix(model_path.suffix+".json").read_text())
    if meta.get("format") != "temporal-gan-v1":
        raise ValueError("Unsupported temporal model format")
    input_hash = file_hash(input_path)
    if input_hash in meta["training_pcap_sha256"].values():
        raise ValueError("Training leakage: evaluation capture occurs in training inputs")
    _, lengths = _observe(input_path, max_packets=None)
    sequence_length = int(meta["sequence_length"])
    generator, _ = build_models(sequence_length, int(meta["hidden_size"]))
    generator.load_state_dict(torch.load(str(model_path), map_location="cpu", weights_only=True))
    generator.to(device).eval()
    _seed_all(seed)
    condition = ((lengths - float(meta["condition_mean"])) / float(meta["condition_std"])).astype(np.float32)
    results = np.empty(len(lengths), dtype=np.float64)
    with torch.no_grad():
        for position in range(0, len(lengths), sequence_length):
            chunk = condition[position:position+sequence_length]
            batch = np.zeros((1, sequence_length, 1), dtype=np.float32)
            batch[0, :len(chunk), 0] = chunk
            cond_t = torch.from_numpy(batch).to(device)
            z = torch.randn((1, sequence_length, 2), device=device)
            generated = generator(z, cond_t)[0, :len(chunk), 0].cpu().numpy()
            results[position:position+len(chunk)] = np.expm1(
                np.clip(generated.astype(float), 0.0, 30.0)) / 1000.0
    return np.maximum(results, 0.0)


def _selection(n: int, fraction: int, seed: int, pattern: str) -> np.ndarray:
    amount = int(round(n * fraction / 100))
    chosen = np.zeros(n, dtype=bool)
    if amount == 0:
        return chosen
    if pattern == "trailing":
        chosen[-amount:] = True
    elif pattern == "distributed":
        rng = np.random.default_rng(seed)
        chosen[rng.choice(n, size=amount, replace=False)] = True
    elif pattern == "blocks":
        # Spread contiguous blocks across the trace, not individual packets.
        rng = np.random.default_rng(seed)
        block = max(1, n // 100)
        while chosen.sum() < amount:
            start = int(rng.integers(0, max(1, n-block+1)))
            candidates = np.flatnonzero(~chosen[start:start+block]) + start
            chosen[candidates[:amount-int(chosen.sum())]] = True
    else:
        raise ValueError("pattern must be trailing, distributed or blocks")
    return chosen


def generate(input_path: Path, model: Path, output: Path, percentage: int,
             seed: int = 42, pattern: str = "trailing",
             duration_policy: str = "match_original", device: str = "cpu") -> dict:
    _, Reader, Writer = _deps()
    if not 0 <= percentage <= 100:
        raise ValueError("percentage must be within 0..100")
    if output.resolve() == input_path.resolve():
        raise ValueError("Refusing to overwrite input")
    if output.exists():
        raise FileExistsError(output)
    _, _ = _deps()
    generated = _generated_gaps(input_path, model, seed, device)
    # Read original gaps separately and stream second pass to writer.
    timestamps = []
    with Reader(str(input_path)) as reader:
        for p in reader:
            timestamps.append(float(p.time))
    original = np.diff(np.asarray(timestamps, dtype=float))
    if len(generated) != len(original):
        raise RuntimeError("Generated gap count mismatch")
    if np.any(original < 0):
        raise ValueError("Input capture has backwards timestamps; fix or exclude first")
    mask = _selection(len(original), percentage, seed, pattern)
    mixed = original.copy()
    mixed[mask] = generated[mask]
    if duration_policy == "match_original":
        if mixed.sum() <= 0 and original.sum() > 0:
            raise ValueError("Generated durations are zero")
        if mixed.sum() > 0:
            mixed *= original.sum() / mixed.sum()
    elif duration_policy != "free":
        raise ValueError("duration_policy must be match_original or free")
    output.parent.mkdir(parents=True, exist_ok=True)
    new_times = np.r_[timestamps[0], timestamps[0] + np.cumsum(mixed)]
    try:
        with Reader(str(input_path)) as reader, Writer(str(output), sync=True) as writer:
            for i, p in enumerate(reader):
                p.time = float(new_times[i])
                writer.write(p)
    except Exception:
        output.unlink(missing_ok=True)
        raise
    meta = {"input": str(input_path.resolve()), "input_sha256": file_hash(input_path),
            "model": str(model.resolve()), "model_sha256": file_hash(model),
            "output": str(output.resolve()), "output_sha256": file_hash(output),
            "seed": seed, "requested_replacement_pct": percentage,
            "actual_gap_replacement_pct": round(float(mask.mean())*100, 6),
            "replaced_gap_count": int(mask.sum()), "packet_count": len(timestamps),
            "pattern": pattern, "duration_policy": duration_policy,
            "original_duration_seconds": float(original.sum()),
            "output_duration_seconds": float(mixed.sum()),
            "experiment_type": "timing_only_sequential_gan",
            "generated_at_utc": datetime.now(timezone.utc).isoformat()}
    output.with_suffix(output.suffix+".json").write_text(json.dumps(meta, indent=2)+"\n")
    return meta


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Opt-in sequential GAN-based PCAP timing generation")
    commands = parser.add_subparsers(dest="command", required=True)
    train_cmd = commands.add_parser("train")
    train_cmd.add_argument("--inputs", nargs="+", required=True, type=Path)
    train_cmd.add_argument("--model", required=True, type=Path)
    train_cmd.add_argument("--epochs", type=int, default=20)
    train_cmd.add_argument("--seed", type=int, default=42)
    train_cmd.add_argument("--device", default="cpu")
    train_cmd.add_argument("--sequence-length", type=int, default=32)
    train_cmd.add_argument("--max-training-packets", type=int, default=300000)
    sweep_cmd = commands.add_parser("sweep")
    sweep_cmd.add_argument("--input", required=True, type=Path)
    sweep_cmd.add_argument("--model", required=True, type=Path)
    sweep_cmd.add_argument("--out", required=True, type=Path)
    sweep_cmd.add_argument("--percentages", default="0,10,20,30,40,50,60,70,80,90,100")
    sweep_cmd.add_argument("--seeds", default="42")
    sweep_cmd.add_argument("--pattern", choices=["trailing", "distributed", "blocks"], default="trailing")
    sweep_cmd.add_argument("--duration-policy", choices=["match_original", "free"], default="match_original")
    sweep_cmd.add_argument("--device", default="cpu")
    args = parser.parse_args(argv)
    if args.command == "train":
        if args.model.exists():
            parser.error("model already exists; refusing overwrite")
        result = train(args.inputs, args.model, args.epochs, args.seed, args.sequence_length,
                       args.device, args.max_training_packets)
        print(json.dumps({k:v for k,v in result.items() if k != "history"}, indent=2))
    else:
        if args.out.exists() and any(args.out.iterdir()):
            parser.error("output directory must be new or empty")
        for seed in map(int, args.seeds.split(",")):
            for pct in map(int, args.percentages.split(",")):
                dest = args.out / f"seed_{seed}" / f"temporal_{pct:03d}.pcap"
                result = generate(args.input, args.model, dest, pct, seed, args.pattern,
                                  args.duration_policy, args.device)
                print(json.dumps({"output": result["output"], "pct": pct, "seed": seed}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
