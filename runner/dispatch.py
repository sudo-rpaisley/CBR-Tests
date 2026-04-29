from __future__ import annotations
from pathlib import Path
import pandas as pd

from tests.pearson_profile import validate_candidate_fields, compute_pearson_profile
from tests.column_quality_profile import compute_column_quality_profile
from tests.timestamp_coherence_profile import run_timestamp_coherence_metric
from tests.metrics.dataset_heuristics.protocol_and_network_realism.address_validity.valid_ip_address_profile import run_protocol_validity_metric
from tests.metrics.dataset_heuristics.protocol_and_network_realism.address_validity.reserved_ip_address_profile import run_reserved_ip_address_metric
from tests.metrics.dataset_heuristics.protocol_and_network_realism.port_validity.valid_port_range_profile import run_valid_port_range_metric
from tests.metrics.dataset_heuristics.protocol_and_network_realism.port_validity.service_port_consistency_profile import run_service_port_consistency_metric
from tests.metrics.dataset_heuristics.protocol_and_network_realism.flow_semantics.packet_byte_consistency_profile import run_packet_byte_consistency_metric
from tests.metrics.dataset_heuristics.protocol_and_network_realism.flow_semantics.flow_duration_consistency_profile import run_flow_duration_consistency_metric
from tests.metrics.dataset_heuristics.protocol_and_network_realism.flow_semantics.handshake_plausibility_profile import run_handshake_plausibility_metric
from tests.metrics.dataset_heuristics.protocol_and_network_realism.flow_semantics.tcp_flag_consistency_profile import run_tcp_flag_consistency_metric
from tests.metrics.dataset_heuristics.protocol_and_network_realism.slice_metadata_integrity.slice_identifier_consistency_profile import run_slice_identifier_consistency_metric
from tests.metrics.dataset_heuristics.protocol_and_network_realism.slice_metadata_integrity.valid_slice_identifier_profile import run_valid_slice_identifier_metric

METRIC_REGISTRY = {}


def register_metric(metric_id: str):
    def _decorator(fn):
        METRIC_REGISTRY[metric_id] = fn
        return fn
    return _decorator


def run_pearson_metric(dataset_path: Path, metric: dict, load_tabular_dataset, shared_df: pd.DataFrame | None = None):
    df = shared_df.copy() if shared_df is not None else load_tabular_dataset(dataset_path)
    candidate_fields = metric['input_requirements']['candidate_fields']
    minimum_runnable_fields = metric['input_requirements']['minimum_runnable_fields']
    column_validation, runnable_fields, df = validate_candidate_fields(df, candidate_fields)
    if len(runnable_fields) < minimum_runnable_fields:
        return False, {'column_validation': column_validation, 'error': 'Not enough usable numeric columns to compute Pearson correlation.'}
    pearson_profile = compute_pearson_profile(df, runnable_fields)
    return True, {'column_validation': column_validation, 'test_results': {'pearson_correlation_profile': pearson_profile}}


def run_column_quality_metric(dataset_path: Path, metric: dict, load_tabular_dataset, shared_df: pd.DataFrame | None = None):
    df = shared_df.copy() if shared_df is not None else load_tabular_dataset(dataset_path)
    candidate_fields = metric['input_requirements']['candidate_fields']
    quality_profile = compute_column_quality_profile(df, candidate_fields)
    return True, {'test_results': {'column_quality_profile': quality_profile}}

@register_metric('timestamp_coherence_profile')
def _timestamp_metric(dataset_path: Path, metric: dict):
    return run_timestamp_coherence_metric(dataset_path, metric)

@register_metric('protocol_validity_profile')
def _protocol_metric(dataset_path: Path, metric: dict):
    return run_protocol_validity_metric(dataset_path, metric)

@register_metric('reserved_ip_address_profile')
def _reserved_ip_metric(dataset_path: Path, metric: dict):
    return run_reserved_ip_address_metric(dataset_path, metric)

@register_metric('valid_port_range_profile')
def _valid_port_metric(dataset_path: Path, metric: dict):
    return run_valid_port_range_metric(dataset_path, metric)

@register_metric('service_port_consistency_profile')
def _service_port_metric(dataset_path: Path, metric: dict):
    return run_service_port_consistency_metric(dataset_path, metric)

@register_metric('tcp_flag_consistency_profile')
def _tcp_flag_metric(dataset_path: Path, metric: dict):
    return run_tcp_flag_consistency_metric(dataset_path, metric)

@register_metric('handshake_plausibility_profile')
def _handshake_metric(dataset_path: Path, metric: dict):
    return run_handshake_plausibility_metric(dataset_path, metric)

@register_metric('flow_duration_consistency_profile')
def _flow_duration_metric(dataset_path: Path, metric: dict):
    return run_flow_duration_consistency_metric(dataset_path, metric)

@register_metric('packet_byte_consistency_profile')
def _packet_byte_metric(dataset_path: Path, metric: dict):
    return run_packet_byte_consistency_metric(dataset_path, metric)

@register_metric('valid_slice_identifier_profile')
def _slice_valid_metric(dataset_path: Path, metric: dict):
    return run_valid_slice_identifier_metric(dataset_path, metric)

@register_metric('slice_identifier_consistency_profile')
def _slice_consistency_metric(dataset_path: Path, metric: dict):
    return run_slice_identifier_consistency_metric(dataset_path, metric)


def build_metric_handlers(shared_df: pd.DataFrame | None, load_tabular_dataset):
    handlers = dict(METRIC_REGISTRY)
    handlers.update({
        'pearson_correlation_profile': lambda dp, m: run_pearson_metric(dp, m, load_tabular_dataset, shared_df),
        'column_quality_profile': lambda dp, m: run_column_quality_metric(dp, m, load_tabular_dataset, shared_df),
    })
    return handlers
