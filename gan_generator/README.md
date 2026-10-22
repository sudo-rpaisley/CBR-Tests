# CTGAN PCAP generator — bundled for remote installation

The trained-model and packet-perturbation CLI is now included in the
**CBR-Tests feature branch**. No ZIP transfer from a Windows machine is needed.

`ctgan-pcap-same-count-source.tar.xz` is a pinned copy of the tested generator
project including its original README, four Python modules and 15 unit tests.
The top-level `traffic_gan.py` wrapper runs those four source modules from a
temporary directory, so the user-facing commands work from the repository root
and do not write generated Python files into Git-tracked directories.

Pinned source SHA-256:

```text
05b977017302d54a2c3d29c529d973ade54379c8946a4c7aea7d3a27aee9f115
```

## Setup on NVIDIA Spark (from the CBR-Tests checkout)

```bash
git fetch origin
git switch feature/gan-perturbation-runner-20261021
git pull --ff-only
source .venv/bin/activate
python -m pip install -r gan_generator/requirements.txt
python traffic_gan.py --help
```

Install CUDA-compatible PyTorch for the Spark platform; check with:

```bash
python -c 'import torch; print(torch.cuda.is_available())'
```

If it prints `False`, use `--device cpu` until PyTorch and the driver are
properly configured. PyTorch compatibility depends on your host/architecture.

## Training and evaluation

The research design uses **three independent capture roles**:

- Dataset 1: fixed reference for all taxonomy comparisons.
- Dataset 2: comparison capture; only its trailing packets get replaced.
- Training PCAP: separate capture from which CTGAN learns flow summaries.

Train from the repository root:

```bash
python traffic_gan.py extract --input /data/training.pcapng --features ./training_flows.csv
python traffic_gan.py train --features ./training_flows.csv --model ./ctgan.pkl --epochs 100 --device cpu --seed 42
```

Alternatively, **no manual training commands are necessary**: open
`python run_plan.py --tui`, choose **GAN perturbation experiment**, select
Dataset 1 and Dataset 2 and choose **Train new CTGAN from PCAPs** (the default).
Use the **Training PCAPs (one or more)** selector to check independent PCAPs
with Space; press `d` to finish. The runner will extract and combine their
flow features, train one model inside `<experiment output>/training/ctgan.pkl`,
then use it for every GAN percentage. Choose CPU or CUDA, epochs and batch size
in the same screen. Training source hashes and checkpoint identity are recorded.

You can still select **Use existing CTGAN checkpoint** to reuse a trusted
`ctgan.pkl`. **Test plan source** defaults to **Auto-create comparison plan**
from Dataset 1 (fixed reference) and Dataset 2 (comparison candidate), so no
existing reference-enabled plan is needed. The generated plan and metric
selection report are saved inside the experiment output directory. Choose
**Use existing PCAP plan** only when you have an appropriate frozen plan
containing reference-comparison metrics.

**Run scripts with a single line, or do not leave a blank line between Bash
lines ending in a backslash.**

The complete research prototype remains intentionally basic: CTGAN conditions
packet-template rewriting, *not* full network conversation generation. It
generates random payload bytes, preserves original timestamps, and retains the
original count of packets while balancing output file size. Do not interpret
the 100% condition as independent end-to-end generated packet conversations.

For inspection or modification of the pinned source, extract the original files:

```bash
mkdir -p /tmp/cbr-ctgan-src
tar -xJf gan_generator/ctgan-pcap-same-count-source.tar.xz -C /tmp/cbr-ctgan-src
ls /tmp/cbr-ctgan-src/ctgan-pcap-same-count
```

Do not commit trained models, dataset PCAPs or unredacted packet payloads to
the public repository.
