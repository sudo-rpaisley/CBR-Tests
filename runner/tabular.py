from __future__ import annotations

from pathlib import Path


def load_tabular_dataset(dataset_path: Path, progress_callback=None):
    import pandas as pd

    suffix = dataset_path.suffix.lower()

    if suffix in {".csv", ".tsv"}:
        sep = "," if suffix == ".csv" else "\t"
        chunk_iter = pd.read_csv(dataset_path, sep=sep, skipinitialspace=True, low_memory=False, chunksize=250_000)
        chunks = []
        total_rows = 0
        for idx, chunk in enumerate(chunk_iter, start=1):
            chunks.append(chunk)
            total_rows += len(chunk)
            if progress_callback:
                progress_callback(idx, total_rows)
        df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    elif suffix in [".xlsx", ".xls"]:
        df = pd.read_excel(dataset_path)
    else:
        raise ValueError(f"Unsupported tabular dataset format: {suffix}")

    df.columns = df.columns.str.strip()
    return df
