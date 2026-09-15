from __future__ import annotations

import os
from pathlib import Path

import pandas as pd


DEFAULT_MAX_SHARED_WORKERS = 4
DEFAULT_FREE_MEMORY_FRACTION_FOR_WORKERS = 0.50


def available_memory_bytes() -> int | None:
    """Return currently available physical memory without adding a dependency.

    Linux ``MemAvailable`` is preferred because it accounts for reclaimable
    caches and is the value most relevant to the experiment hosts.  A POSIX
    ``sysconf`` fallback is retained for environments where /proc is absent.
    """
    meminfo = Path("/proc/meminfo")
    try:
        if meminfo.exists():
            for line in meminfo.read_text(encoding="utf-8").splitlines():
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass

    try:
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
        available_pages = int(os.sysconf("SC_AVPHYS_PAGES"))
        if page_size > 0 and available_pages > 0:
            return page_size * available_pages
    except (AttributeError, OSError, ValueError):
        pass
    return None


def dataframe_memory_bytes(dataframe: pd.DataFrame | None) -> int | None:
    if dataframe is None:
        return None
    return int(dataframe.memory_usage(index=True, deep=True).sum())


def choose_worker_policy(
    *,
    requested_workers: int,
    shared_dataframe: pd.DataFrame | None,
    available_bytes: int | None = None,
    max_shared_workers: int = DEFAULT_MAX_SHARED_WORKERS,
    free_memory_fraction_for_workers: float = DEFAULT_FREE_MEMORY_FRACTION_FOR_WORKERS,
) -> dict:
    """Return a transparent worker decision for a loaded experiment dataset.

    Parallel metrics can allocate temporary numeric arrays or narrow dataframe
    copies.  The previous fixed cap of four workers did not take the resident
    dataset size into account, so four workers could be safe for a small CSV but
    unsafe for a multi-gigabyte packet view.

    The policy never samples or changes metric calculations.  It only lowers
    concurrency when the shared dataframe is large relative to memory that is
    still available after loading.  The complete decision is returned so it can
    be written into experiment provenance.
    """
    requested = max(1, int(requested_workers))
    if max_shared_workers < 1:
        raise ValueError("max_shared_workers must be at least 1")
    if not 0 < free_memory_fraction_for_workers <= 1:
        raise ValueError("free_memory_fraction_for_workers must be in (0, 1]")

    frame_bytes = dataframe_memory_bytes(shared_dataframe)
    detected_available = (
        available_memory_bytes() if available_bytes is None else max(0, int(available_bytes))
    )

    if shared_dataframe is None:
        return {
            "requested_workers": requested,
            "effective_workers": requested,
            "worker_cap": None,
            "cap_reason": None,
            "shared_dataframe_bytes": None,
            "available_memory_bytes": detected_available,
            "free_memory_fraction_for_workers": free_memory_fraction_for_workers,
        }

    fixed_cap = min(requested, max_shared_workers)
    memory_cap = max_shared_workers
    cap_reason = "shared_dataframe_fixed_cap" if requested > max_shared_workers else None

    if frame_bytes and detected_available is not None:
        worker_budget = int(detected_available * free_memory_fraction_for_workers)
        memory_cap = max(1, worker_budget // frame_bytes)
        memory_cap = min(max_shared_workers, memory_cap)
        if memory_cap < fixed_cap:
            cap_reason = "shared_dataframe_memory_budget"

    effective = max(1, min(fixed_cap, memory_cap))
    return {
        "requested_workers": requested,
        "effective_workers": effective,
        "worker_cap": min(max_shared_workers, memory_cap),
        "cap_reason": cap_reason,
        "shared_dataframe_bytes": frame_bytes,
        "available_memory_bytes": detected_available,
        "free_memory_fraction_for_workers": free_memory_fraction_for_workers,
        "max_shared_workers": max_shared_workers,
    }
