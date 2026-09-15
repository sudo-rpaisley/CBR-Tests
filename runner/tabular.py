from __future__ import annotations

from pathlib import Path


def load_tabular_dataset(
    dataset_path: Path,
    progress_callback=None,
    field_translation: dict[str, str] | None = None,
):
    """Load a supported tabular dataset into one shared dataframe.

    CSV/TSV inputs are intentionally read in one pandas call.  The previous
    implementation collected every 250,000-row chunk in a Python list and then
    concatenated those chunks, which briefly retained both the chunk collection
    and the newly allocated full dataframe.  On experiment-scale datasets that
    could roughly double the dataframe-side peak memory and trigger the OOM
    killer before any metric ran.

    The runner requires a complete dataframe for metrics whose semantics depend
    on the full ordered population, so chunked metric execution would not be an
    equivalent optimisation.  A direct read therefore gives the same full-data
    semantics with a substantially lower peak allocation.  The progress hook is
    still called once after loading so callers retain a deterministic completion
    update without changing the data path.
    """
    import pandas as pd

    suffix = dataset_path.suffix.lower()

    if suffix in {".csv", ".tsv"}:
        sep = "," if suffix == ".csv" else "\t"
        df = pd.read_csv(
            dataset_path,
            sep=sep,
            skipinitialspace=True,
            low_memory=False,
            memory_map=True,
        )
        if progress_callback:
            progress_callback(1, len(df))
    elif suffix in {".xlsx", ".xls"}:
        df = pd.read_excel(dataset_path)
        if progress_callback:
            progress_callback(1, len(df))
    else:
        raise ValueError(f"Unsupported tabular dataset format: {suffix}")

    df.columns = df.columns.str.strip()
    return df
