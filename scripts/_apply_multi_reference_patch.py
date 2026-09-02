from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly one match, found {count}")
    return text.replace(old, new, 1)


def patch_create_plan() -> None:
    path = ROOT / "create_plan.py"
    text = path.read_text(encoding="utf-8")
    if "def _browse_reference_files()" in text:
        return

    marker = '''def _prompt(\n'''
    insert = '''def _browse_reference_files() -> list[str]:\n    """Interactively collect one or more independent reference datasets."""\n\n    selected: list[str] = []\n    while True:\n        value = _browse_dataset_file()\n        if value is None:\n            if not selected:\n                return []\n            print("Reference selection cancelled; keeping the references already selected.")\n            return selected\n        if value not in selected:\n            selected.append(value)\n            print(f"Selected reference {len(selected)}: {value}")\n        else:\n            print(f"Reference already selected: {value}")\n\n        answer = input("Add another reference dataset? [y/N] ").strip().lower()\n        if answer not in {"y", "yes"}:\n            return selected\n\n\n'''
    text = replace_once(text, marker, insert + marker, "insert reference browser")

    old = '''    parser.add_argument(\n        "--reference-dataset",\n        help="Optional independent reference dataset. Raw-PCAP reference comparison requires a PCAP/PCAPNG reference.",\n    )\n'''
    new = '''    parser.add_argument(\n        "--reference-dataset",\n        action="append",\n        help=(\n            "Optional independent reference dataset. Repeat --reference-dataset to compare every selected "\n            "candidate against several references. Interactive mode uses the file browser instead of typed paths."\n        ),\n    )\n'''
    text = replace_once(text, old, new, "reference argparse")

    start = text.index("    reference_value = reference_arg\n")
    end_marker = "    single_service = single_service_arg\n"
    end = text.index(end_marker, start)
    new_block = '''    if isinstance(reference_arg, (str, Path)):\n        reference_values = [str(reference_arg)]\n    else:\n        reference_values = list(reference_arg or [])\n\n    if not reference_values and sys.stdin.isatty() and sys.stdout.isatty():\n        answer = input(\n            "\\nAdd one or more independent reference datasets for comparison metrics? [y/N] "\n        ).strip().lower()\n        if answer in {"y", "yes"}:\n            reference_values = _browse_reference_files()\n            if not reference_values:\n                print("No references selected; reference-comparison metrics remain excluded.")\n\n    reference_dataset_paths = _deduplicate_dataset_values(reference_values)\n    if all_pcap and any(path.suffix.lower() not in PCAP_SUFFIXES for path in reference_dataset_paths):\n        raise ValueError("Raw-PCAP candidates can only use PCAP/PCAPNG reference datasets.")\n    if not all_pcap and any(path.suffix.lower() in PCAP_SUFFIXES for path in reference_dataset_paths):\n        raise ValueError("Tabular candidates cannot use raw-PCAP reference datasets.")\n\n'''
    text = text[:start] + new_block + text[end:]

    function_start = text.index("def _create_batch(\n")
    function_end = text.index("\ndef parse_args() -> argparse.Namespace:\n", function_start)
    new_function = '''def _create_batch(\n    *,\n    plan_id: str,\n    name: str,\n    description: str,\n    dataset_paths: list[Path],\n    field_translation_path: Path | None,\n    include_metric_ids: list[str] | None,\n    exclude_metric_ids: list[str] | None,\n    reference_dataset_paths: list[Path],\n    service_port_configuration: dict | None,\n    output_path: Path,\n    force: bool,\n    per_dataset_metrics: bool,\n    interactive: bool,\n) -> Path:\n    repo_root = Path.cwd().resolve()\n    batch_plan_dir = output_path.parent / f"{plan_id}_batch_plans"\n    used_slugs: set[str] = set()\n    generated: list[dict] = []\n\n    combinations: list[tuple[Path, Path | None]] = []\n    if reference_dataset_paths:\n        for dataset_path in dataset_paths:\n            for reference_path in reference_dataset_paths:\n                if dataset_path.resolve() == reference_path.resolve():\n                    print(f"Skipping self-comparison: {dataset_path}")\n                    continue\n                combinations.append((dataset_path, reference_path))\n    else:\n        combinations = [(dataset_path, None) for dataset_path in dataset_paths]\n\n    if not combinations:\n        raise ValueError("No runnable candidate/reference combinations remain after excluding self-comparisons.")\n\n    print(\n        f"\\nBuilding batch plan for {len(dataset_paths)} candidate dataset(s), "\n        f"{len(reference_dataset_paths)} reference dataset(s), and {len(combinations)} job(s)"\n    )\n    for index, (dataset_path, reference_path) in enumerate(combinations, start=1):\n        base_slug = _job_slug(dataset_path, index, used_slugs)\n        if reference_path is not None:\n            reference_slug = _slug(reference_path.stem)\n            slug = f"{base_slug}-vs-{reference_slug}"\n            if slug in used_slugs:\n                slug = f"{slug}-{index:02d}"\n            used_slugs.add(slug)\n        else:\n            slug = base_slug\n        child_plan_id = f"{plan_id}-{slug}"\n        child_name = f"{name} — {dataset_path.name}"\n        if reference_path is not None:\n            child_name += f" vs {reference_path.name}"\n        print(f"\\n[{index}/{len(combinations)}] Preflighting {dataset_path}")\n        if reference_path is not None:\n            print(f"    Reference: {reference_path}")\n        plan, report = _build_single_plan(\n            plan_id=child_plan_id,\n            name=child_name,\n            description=description,\n            dataset_path=dataset_path,\n            field_translation_path=field_translation_path,\n            include_metric_ids=include_metric_ids,\n            exclude_metric_ids=exclude_metric_ids,\n            reference_dataset_path=reference_path,\n            service_port_configuration=service_port_configuration,\n        )\n        _print_report(report)\n        generated.append(\n            {\n                "dataset_path": dataset_path,\n                "reference_dataset_path": reference_path,\n                "slug": slug,\n                "plan": plan,\n                "report": report,\n                "plan_path": batch_plan_dir / f"{index:02d}_{slug}_plan.json",\n            }\n        )\n\n    common_metric_ids: set[str] | None = None\n    if not per_dataset_metrics:\n        metric_sets = [\n            {metric["metric_id"] for metric in item["plan"].get("metrics", [])}\n            for item in generated\n        ]\n        common_metric_ids = set.intersection(*metric_sets) if metric_sets else set()\n        if not common_metric_ids:\n            raise ValueError(\n                "No metric is runnable across every selected candidate/reference job. "\n                "Resolve field mappings, choose more compatible datasets, or use --per-dataset-metrics."\n            )\n        print(\n            f"\\nBatch common metric set: {len(common_metric_ids)} tests runnable across all "\n            f"{len(generated)} jobs."\n        )\n        for item in generated:\n            _filter_plan_to_metric_ids(item["plan"], common_metric_ids)\n    else:\n        print("\\nBatch metric policy: each candidate/reference job keeps its own runnable metric set.")\n\n    if interactive:\n        answer = input("\\nSave this dataset batch and its generated plans? [Y/n] ").strip().lower()\n        if answer not in {"", "y", "yes"}:\n            raise KeyboardInterrupt("Batch save cancelled by user.")\n\n    existing_paths = [item["plan_path"] for item in generated if item["plan_path"].exists()]\n    if output_path.exists():\n        existing_paths.append(output_path)\n    overwrite = force\n    if existing_paths and not overwrite and interactive:\n        print("\\nThe following batch files already exist:")\n        for path in existing_paths:\n            print(f"  - {path}")\n        answer = input("Overwrite the existing batch files? [y/N] ").strip().lower()\n        overwrite = answer in {"y", "yes"}\n        if not overwrite:\n            raise KeyboardInterrupt("Batch save cancelled; existing files were left unchanged.")\n\n    if existing_paths and not overwrite:\n        raise FileExistsError(\n            "One or more batch output files already exist. Use --force to replace them."\n        )\n\n    jobs = []\n    for index, item in enumerate(generated, start=1):\n        reference_path = item["reference_dataset_path"]\n        item["plan"].setdefault("plan_creation", {})["batch"] = {\n            "batch_id": plan_id,\n            "batch_name": name,\n            "job_index": index,\n            "job_count": len(generated),\n            "metric_policy": (\n                "per_dataset" if per_dataset_metrics else "common_across_all_datasets"\n            ),\n        }\n        write_plan(item["plan_path"], item["plan"], overwrite=overwrite)\n        jobs.append(\n            {\n                "job_id": f"{plan_id}-{index:02d}-{item['slug']}",\n                "dataset_path": str(item["dataset_path"].resolve()),\n                "reference_dataset_path": (\n                    str(reference_path.resolve()) if reference_path is not None else None\n                ),\n                "plan_path": _portable_path(item["plan_path"], repo_root),\n                "runnable_metric_count": len(item["plan"].get("metrics", [])),\n                "metric_ids": [metric["metric_id"] for metric in item["plan"].get("metrics", [])],\n            }\n        )\n\n    batch_payload = {\n        "schema_version": BATCH_SCHEMA_VERSION,\n        "batch_meta": {\n            "batch_id": plan_id,\n            "name": name,\n            "description": description,\n            "created_at": datetime.now(timezone.utc).isoformat(),\n            "execution_mode": "sequential",\n            "dataset_count": len(dataset_paths),\n            "reference_dataset_count": len(reference_dataset_paths),\n            "job_count": len(jobs),\n            "comparison_mode": "candidate_reference_matrix" if reference_dataset_paths else "dataset_batch",\n            "metric_policy": (\n                "per_dataset" if per_dataset_metrics else "common_across_all_datasets"\n            ),\n        },\n        "output_directory": str(Path("outcomes") / plan_id),\n        "common_metric_ids": sorted(common_metric_ids) if common_metric_ids is not None else None,\n        "reference_datasets": [str(path.resolve()) for path in reference_dataset_paths],\n        "reference_dataset": (\n            str(reference_dataset_paths[0].resolve()) if len(reference_dataset_paths) == 1 else None\n        ),\n        "jobs": jobs,\n    }\n    written = _write_json_atomic(output_path, batch_payload, overwrite=overwrite)\n    print(f"\\nBatch manifest written: {written}")\n    print(f"Generated per-job plans: {batch_plan_dir.resolve()}")\n    print(f"Run the batch with: python run_batch.py --batch {written}")\n    return written\n\n'''
    text = text[:function_start] + new_function + text[function_end:]

    old_condition = "    if len(dataset_paths) > 1:\n"
    new_condition = "    if len(dataset_paths) > 1 or len(reference_dataset_paths) > 1:\n"
    text = replace_once(text, old_condition, new_condition, "batch condition")

    old_print = '''        if reference_dataset_path:\n            print(f"Reference:    {reference_dataset_path}")\n'''
    new_print = '''        if reference_dataset_paths:\n            print(f"References:   {len(reference_dataset_paths)}")\n            for index, path in enumerate(reference_dataset_paths, start=1):\n                print(f"  R{index:>2}. {path}")\n'''
    text = replace_once(text, old_print, new_print, "batch reference display")

    old_call = "                reference_dataset_path=reference_dataset_path,\n"
    new_call = "                reference_dataset_paths=reference_dataset_paths,\n"
    text = replace_once(text, old_call, new_call, "batch reference argument")

    old_single_marker = "    dataset_path = dataset_paths[0]\n"
    new_single_marker = '''    dataset_path = dataset_paths[0]\n    reference_dataset_path = reference_dataset_paths[0] if reference_dataset_paths else None\n'''
    text = replace_once(text, old_single_marker, new_single_marker, "single reference selection")

    path.write_text(text, encoding="utf-8")


def patch_plan_builder() -> None:
    path = ROOT / "runner" / "plan_builder.py"
    text = path.read_text(encoding="utf-8")
    if "def tabular_reference_metric_template(" in text:
        return

    old_import = '''    detect_standard_pcap_field_translation_for_dataset,\n    load_field_translation,\n    merge_field_translations,\n    read_tabular_dataset_columns,\n)\n'''
    new_import = '''    detect_standard_pcap_field_translation_for_dataset,\n    field_resolver,\n    load_field_translation,\n    merge_field_translations,\n    read_tabular_dataset_columns,\n)\n'''
    text = replace_once(text, old_import, new_import, "field_resolver import")

    marker = "def inspect_dataset(\n"
    helpers = '''def _canonical_fields(columns: list[str], translation: dict[str, str]) -> set[str]:\n    return {translation.get(column, column) for column in columns}\n\n\ndef _sample_numeric_fields(\n    dataset_path: Path,\n    columns: list[str],\n    translation: dict[str, str],\n    *,\n    sample_rows: int = 250,\n) -> set[str]:\n    """Identify numeric-compatible canonical fields from a small deterministic prefix sample."""\n\n    if not columns:\n        return set()\n    import pandas as pd\n\n    suffix = dataset_path.suffix.lower()\n    try:\n        if suffix in {".csv", ".tsv"}:\n            frame = pd.read_csv(\n                dataset_path,\n                sep="\\t" if suffix == ".tsv" else ",",\n                skipinitialspace=True,\n                low_memory=False,\n                nrows=sample_rows,\n            )\n        else:\n            frame = pd.read_excel(dataset_path, nrows=sample_rows)\n    except Exception:\n        return set()\n\n    frame.columns = [str(column).strip() for column in frame.columns]\n    numeric: set[str] = set()\n    for raw_field in frame.columns:\n        series = frame[raw_field]\n        non_null = series.dropna()\n        if len(non_null) < 2:\n            continue\n        converted = pd.to_numeric(non_null, errors="coerce")\n        if float(converted.notna().mean()) >= 0.8:\n            numeric.add(translation.get(raw_field, raw_field))\n    return numeric\n\n\ndef _reference_field_map(candidate: dict, reference: dict, fields: list[str]) -> dict[str, str]:\n    candidate_resolver = field_resolver(candidate.get("field_translation", {}), candidate.get("columns", []))\n    reference_resolver = field_resolver(reference.get("field_translation", {}), reference.get("columns", []))\n    mapping: dict[str, str] = {}\n    for canonical in fields:\n        candidate_field = candidate_resolver.get(canonical, canonical)\n        reference_field = reference_resolver.get(canonical, canonical)\n        if candidate_field != reference_field:\n            mapping[reference_field] = candidate_field\n    return mapping\n\n\ndef tabular_reference_metric_template(\n    metric_id: str,\n    label: str,\n    candidate: dict,\n    reference: dict,\n) -> dict | None:\n    """Build a reference-comparison template from fields shared by two tabular datasets."""\n\n    common_fields = set(candidate.get("canonical_fields", set())) & set(reference.get("canonical_fields", set()))\n    common_numeric = sorted(\n        set(candidate.get("numeric_fields", set())) & set(reference.get("numeric_fields", set()))\n    )\n    reference_path = str(reference["path"])\n    requirements: dict = {"reference_dataset_path": reference_path}\n    parameters: dict = {}\n\n    feature_metrics = {\n        "feature_wise_wasserstein_distance_from_reference",\n        "feature_wise_ks_statistic_from_reference",\n        "feature_wise_energy_distance_from_reference",\n        "flow_statistic_deviation_from_reference",\n    }\n    matrix_metrics = {\n        "feature_set_mmd_score_from_reference",\n        "pearson_matrix_deviation_from_reference",\n        "spearman_matrix_deviation_from_reference",\n        "distance_correlation_matrix_deviation_from_reference",\n    }\n    temporal_metrics = {\n        "inter_arrival_distribution_divergence_from_reference",\n        "burstiness_deviation_from_reference",\n        "hourly_activity_divergence_from_reference",\n    }\n\n    required_fields: list[str] = []\n    if metric_id in feature_metrics:\n        if not common_numeric:\n            return None\n        requirements["candidate_fields"] = common_numeric\n        required_fields = list(common_numeric)\n        parameters["max_sample_size"] = 1000\n    elif metric_id in matrix_metrics:\n        if len(common_numeric) < 2:\n            return None\n        requirements["candidate_fields"] = common_numeric\n        required_fields = list(common_numeric)\n        parameters["max_sample_size"] = 500 if metric_id == "feature_set_mmd_score_from_reference" else 1000\n    elif metric_id in temporal_metrics:\n        timestamp_field = "timestamp" if "timestamp" in common_fields else "Timestamp" if "Timestamp" in common_fields else None\n        if timestamp_field is None:\n            return None\n        requirements["timestamp_field"] = timestamp_field\n        required_fields = [timestamp_field]\n    elif metric_id == "protocol_mix_divergence_from_reference":\n        if "Protocol" not in common_fields:\n            return None\n        requirements["protocol_field"] = "Protocol"\n        required_fields = ["Protocol"]\n    elif metric_id == "port_use_divergence_from_reference":\n        port_fields = [field for field in ("Source Port", "Destination Port") if field in common_fields]\n        if not port_fields:\n            return None\n        requirements["port_fields"] = port_fields\n        required_fields = list(port_fields)\n    elif metric_id == "slice_proportion_deviation_from_reference":\n        if "slice" not in common_fields:\n            return None\n        requirements["slice_field"] = "slice"\n        required_fields = ["slice"]\n    elif metric_id == "per_slice_class_divergence_from_reference":\n        if not {"slice", "label"}.issubset(common_fields):\n            return None\n        requirements.update({"slice_field": "slice", "label_field": "label"})\n        required_fields = ["slice", "label"]\n    elif metric_id == "per_slice_feature_distribution_deviation_from_reference":\n        if "slice" not in common_fields or not common_numeric:\n            return None\n        requirements.update({"slice_field": "slice", "candidate_fields": common_numeric})\n        required_fields = ["slice", *common_numeric]\n    else:\n        return None\n\n    mapping_fields = [field for field in required_fields if field in common_fields]\n    template = {\n        "metric_id": metric_id,\n        "label": label,\n        "input_requirements": requirements,\n        "field_requirements": {"required": required_fields},\n        "calculation": {\n            "method": f"tabular_{metric_id}",\n            "parameters": parameters,\n        },\n    }\n    reference_map = _reference_field_map(candidate, reference, mapping_fields)\n    if reference_map:\n        template["reference_field_map"] = reference_map\n    return template\n\n\n'''
    text = replace_once(text, marker, helpers + marker, "insert tabular reference helpers")

    old_return = '''    return {\n        "path": dataset_path,\n        "format": dataset_format(dataset_path),\n        "columns": columns,\n        "available_fields": fields,\n        "field_translation": translation,\n        "field_translation_path": resolved_translation_path,\n    }\n'''
    new_return = '''    canonical_fields = (\n        _canonical_fields(columns, translation)\n        if columns\n        else set(PCAP_PACKET_COLUMNS) if suffix in PCAP_SUFFIXES else set()\n    )\n    numeric_fields = (\n        _sample_numeric_fields(dataset_path, columns, translation)\n        if suffix in TABULAR_SUFFIXES\n        else {"Timestamp", "Source Port", "Destination Port", "Protocol", "IP Version", "Packet Length", "TCP Flags", "Inter Arrival Time"}\n        if suffix in PCAP_SUFFIXES\n        else set()\n    )\n    return {\n        "path": dataset_path,\n        "format": dataset_format(dataset_path),\n        "columns": columns,\n        "available_fields": fields,\n        "canonical_fields": canonical_fields,\n        "numeric_fields": numeric_fields,\n        "field_translation": translation,\n        "field_translation_path": resolved_translation_path,\n    }\n'''
    text = replace_once(text, old_return, new_return, "inspect dataset fields")

    manual_marker = '''    if manual_reason:\n        return "needs_configuration", manual_reason, []\n'''
    tabular_state = '''    if not is_pcap and metric_id.endswith("_from_reference"):\n        if reference_dataset is None:\n            return "needs_configuration", "reference_dataset_required", []\n        if reference_dataset.get("format") not in {"csv", "tsv", "xlsx", "xls"}:\n            return "needs_configuration", "reference_representation_mismatch", []\n        if template is None:\n            return "needs_configuration", "reference_shared_fields_missing", []\n        required = required_fields(template)\n        available = dataset.get("available_fields", set())\n        missing = [field for field in required if field not in available]\n        if missing:\n            return "needs_mapping", "required_fields_not_resolved", missing\n        reference_available = reference_dataset.get("available_fields", set())\n        reference_missing = [field for field in required if field not in reference_available]\n        if reference_missing:\n            return "needs_mapping", "reference_required_fields_not_resolved", reference_missing\n        return "ready", None, []\n\n'''
    text = replace_once(text, manual_marker, tabular_state + manual_marker, "tabular reference configuration state")

    injection_marker = '''        if dataset["format"] in {"pcap", "pcapng"}:\n            if metric_id in PCAP_PACKET_METRICS:\n'''
    replacement = '''        if dataset["format"] in {"pcap", "pcapng"}:\n            if metric_id in PCAP_PACKET_METRICS:\n'''
    # Keep the existing PCAP block and inject tabular handling immediately after it using a later anchor.
    if injection_marker not in text:
        raise RuntimeError("PCAP metric block anchor not found")

    anchor = '''                if service_port_configuration:\n                    spec["template"] = pcap_service_port_template(\n                        service_port_configuration.get("service_name", ""),\n                        service_port_configuration.get("expected_ports", []),\n                    )\n\n        state, reason, missing = _configuration_state(spec, dataset, reference_dataset)\n'''
    new_anchor = '''                if service_port_configuration:\n                    spec["template"] = pcap_service_port_template(\n                        service_port_configuration.get("service_name", ""),\n                        service_port_configuration.get("expected_ports", []),\n                    )\n        elif reference_dataset is not None and metric_id.endswith("_from_reference"):\n            spec = dict(spec)\n            spec["template"] = tabular_reference_metric_template(\n                metric_id,\n                spec["label"],\n                dataset,\n                reference_dataset,\n            )\n\n        state, reason, missing = _configuration_state(spec, dataset, reference_dataset)\n'''
    text = replace_once(text, anchor, new_anchor, "inject tabular reference template")

    old_ref_plan = '''    plan["plan_creation"]["reference_dataset_format"] = (\n        reference_dataset["format"] if reference_dataset is not None else None\n    )\n'''
    new_ref_plan = '''    plan["plan_creation"]["reference_dataset_format"] = (\n        reference_dataset["format"] if reference_dataset is not None else None\n    )\n    plan["plan_creation"]["reference_field_translation"] = (\n        str(reference_dataset["field_translation_path"])\n        if reference_dataset is not None and reference_dataset.get("field_translation_path") is not None\n        else None\n    )\n'''
    text = replace_once(text, old_ref_plan, new_ref_plan, "reference translation provenance")

    path.write_text(text, encoding="utf-8")


def patch_reference_loader() -> None:
    path = ROOT / "tests" / "reference_model_comparison_profile.py"
    text = path.read_text(encoding="utf-8")
    if "def _apply_reference_field_map(" in text:
        return

    marker = '''_REFERENCE_DF_CACHE_LOCK = Lock()\n\n\ndef _load_reference_df(metric: dict) -> pd.DataFrame:\n'''
    replacement = '''_REFERENCE_DF_CACHE_LOCK = Lock()\n\n\ndef _apply_reference_field_map(dataframe: pd.DataFrame, metric: dict) -> pd.DataFrame:\n    mapping = metric.get("reference_field_map", {})\n    if not isinstance(mapping, dict) or not mapping:\n        return dataframe\n    rename_map = {\n        str(source): str(target)\n        for source, target in mapping.items()\n        if str(source).strip() and str(target).strip() and source != target\n    }\n    collisions = [\n        target\n        for source, target in rename_map.items()\n        if target in dataframe.columns and target not in rename_map\n    ]\n    if collisions:\n        raise ValueError(\n            "Reference field mapping would overwrite existing columns: " + ", ".join(sorted(set(collisions)))\n        )\n    return dataframe.rename(columns=rename_map)\n\n\ndef _load_reference_df(metric: dict) -> pd.DataFrame:\n'''
    text = replace_once(text, marker, replacement, "reference map helper")

    text = replace_once(
        text,
        '''    if isinstance(shared, pd.DataFrame):\n        return shared\n''',
        '''    if isinstance(shared, pd.DataFrame):\n        return _apply_reference_field_map(shared.copy(), metric)\n''',
        "shared reference mapping",
    )
    text = replace_once(
        text,
        '''        cached = _REFERENCE_DF_CACHE.get(cache_key)\n        if cached is not None:\n            return cached\n''',
        '''        cached = _REFERENCE_DF_CACHE.get(cache_key)\n        if cached is not None:\n            return _apply_reference_field_map(cached.copy(), metric)\n''',
        "cached reference mapping",
    )
    text = replace_once(
        text,
        '''        _REFERENCE_DF_CACHE[cache_key] = dataframe\n        return dataframe\n''',
        '''        _REFERENCE_DF_CACHE[cache_key] = dataframe\n        return _apply_reference_field_map(dataframe.copy(), metric)\n''',
        "new reference mapping",
    )
    path.write_text(text, encoding="utf-8")


def patch_run_batch() -> None:
    path = ROOT / "run_batch.py"
    text = path.read_text(encoding="utf-8")
    if "reference_slug =" in text:
        return

    text = replace_once(
        text,
        '''    print(f"Datasets: {len(jobs)}")\n''',
        '''    print(f"Jobs: {len(jobs)}")\n    print(f"Candidate datasets: {meta.get('dataset_count', len(jobs))}")\n    if meta.get("reference_dataset_count"):\n        print(f"Reference datasets: {meta.get('reference_dataset_count')}")\n''',
        "batch header counts",
    )

    old = '''        dataset_slug = _slug(dataset_path.stem)\n        output_path = output_dir / f"outcome_{index:02d}_{dataset_slug}_{timestamp}.json"\n\n        print()\n        print("-" * 88)\n        print(f"[{index}/{len(jobs)}] {dataset_path.name}")\n        print(f"Plan: {plan_path}")\n'''
    new = '''        dataset_slug = _slug(dataset_path.stem)\n        reference_value = job.get("reference_dataset_path")\n        reference_path = _resolve_repo_path(repo_root, str(reference_value)) if reference_value else None\n        reference_slug = f"_vs_{_slug(reference_path.stem)}" if reference_path is not None else ""\n        output_path = output_dir / f"outcome_{index:02d}_{dataset_slug}{reference_slug}_{timestamp}.json"\n\n        print()\n        print("-" * 88)\n        print(f"[{index}/{len(jobs)}] {dataset_path.name}")\n        if reference_path is not None:\n            print(f"Reference: {reference_path.name}")\n        print(f"Plan: {plan_path}")\n'''
    text = replace_once(text, old, new, "reference outcome naming")

    old_result = '''            "dataset_path": str(dataset_path),\n            "plan_path": str(plan_path),\n'''
    new_result = '''            "dataset_path": str(dataset_path),\n            "reference_dataset_path": str(reference_path) if reference_path is not None else None,\n            "plan_path": str(plan_path),\n'''
    text = replace_once(text, old_result, new_result, "reference result provenance")
    path.write_text(text, encoding="utf-8")


def main() -> None:\n    patch_create_plan()\n    patch_plan_builder()\n    patch_reference_loader()\n    patch_run_batch()\n\n\nif __name__ == "__main__":\n    main()\n