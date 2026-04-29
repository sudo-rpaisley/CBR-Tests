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


def build_metric_handlers(shared_df: pd.DataFrame | None, load_tabular_dataset):
    return {
        'pearson_correlation_profile': lambda dp, m: run_pearson_metric(dp, m, load_tabular_dataset, shared_df),
        'column_quality_profile': lambda dp, m: run_column_quality_metric(dp, m, load_tabular_dataset, shared_df),
        'timestamp_coherence_profile': run_timestamp_coherence_metric,
        'protocol_validity_profile': run_protocol_validity_metric,
        'reserved_ip_address_profile': run_reserved_ip_address_metric,
        'valid_port_range_profile': run_valid_port_range_metric,
        'service_port_consistency_profile': run_service_port_consistency_metric,
        'tcp_flag_consistency_profile': run_tcp_flag_consistency_metric,
        'handshake_plausibility_profile': run_handshake_plausibility_metric,
        'flow_duration_consistency_profile': run_flow_duration_consistency_metric,
        'packet_byte_consistency_profile': run_packet_byte_consistency_metric,
        'valid_slice_identifier_profile': run_valid_slice_identifier_metric,
        'slice_identifier_consistency_profile': run_slice_identifier_consistency_metric,
    }
