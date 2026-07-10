from __future__ import annotations
import json
from pathlib import Path


def resolve_path(base_dir: Path, path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def load_case_or_plan(case_file: Path, dataset_arg: str | None, output_arg: str | None, case_id_arg: str, field_translation_arg: str | None = None):
    case_dir = case_file.parent
    with open(case_file, 'r', encoding='utf-8') as f:
        case = json.load(f)

    if 'test_plan' in case and 'dataset' in case and 'output' in case:
        plan_path = resolve_path(case_dir, case['test_plan']['path'])
        dataset_path = resolve_path(case_dir, case['dataset']['path'])
        output_path = resolve_path(case_dir, case['output']['path'])
        case_id = case.get('case_id', 'unknown_case')
        translation_path = None
        translation_spec = case.get('dataset', {}).get('field_translation') or case.get('field_translation')
        if isinstance(translation_spec, dict) and translation_spec.get('path'):
            translation_path = resolve_path(case_dir, translation_spec['path'])
        elif isinstance(translation_spec, str):
            translation_path = resolve_path(case_dir, translation_spec)
        with open(plan_path, 'r', encoding='utf-8') as f:
            plan = json.load(f)
    elif 'metrics' in case and 'plan_meta' in case:
        if not dataset_arg or not output_arg:
            raise ValueError('When --case points to a plan JSON, you must also provide --dataset and --output.')
        plan = case
        dataset_path = Path(dataset_arg).expanduser().resolve()
        output_path = Path(output_arg).expanduser().resolve()
        case_id = case_id_arg
        translation_path = None
    else:
        raise ValueError('Invalid input JSON: provide a case JSON, or a plan JSON with --dataset and --output.')


    if field_translation_arg:
        translation_path = Path(field_translation_arg).expanduser().resolve()

    return plan, dataset_path, output_path, case_id, translation_path
