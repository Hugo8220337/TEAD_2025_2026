import os
import sys
import json
import pandas as pd
import pyarrow as pa
import numpy as np
from typing import Tuple
from pyiceberg.catalog import load_catalog
from flytekit import ImageSpec, task, workflow

# Access parent folder to import utils
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from utils.config import SETTINGS
from utils.flyte_utils import quality_gate, write_dataframe_to_iceberg

SILVER_CLEAN_ID = f"{SETTINGS.namespace_silver}.{SETTINGS.table_vitals_silver_clean}"
SILVER_QUARANTINE_ID = f"{SETTINGS.namespace_silver}.{SETTINGS.table_vitals_silver_quarantine}"
SILVER_METADATA_ID = f"{SETTINGS.namespace_silver}.{SETTINGS.table_vitals_silver_metadata}"

image_spec_gold = ImageSpec(
    name="sensor_quality_and_anomalies_gold",
    registry="localhost:30000",
    packages=[
        "flytekit==1.10.0",
        "pandas>=2.0.0",
        "pyarrow>=10.0.1",
        "pyiceberg>=0.6.0",
        "s3fs>=2023.12.2",
        "numpy<2.0.0",
    ],
)

def _unpivot_vitals_data(clean_df: pd.DataFrame) -> pd.DataFrame:
    """Transforms wide format into long format (sensor_name, reading)."""
    id_vars = [col for col in ['case_id', 'time', 'ingest_time'] if col in clean_df.columns]
    long_df = clean_df.melt(id_vars=id_vars, var_name='sensor_name', value_name='reading')
    long_df['reading'] = pd.to_numeric(long_df['reading'], errors='coerce')
    return long_df

def _calculate_uptime_metrics(long_df: pd.DataFrame) -> pd.DataFrame:
    """Calculates true uptime based on the sensor's specific active window."""
    valid_df = long_df.dropna(subset=['reading'])
    
    # Calculate active window and count valid readings
    sensor_window = valid_df.groupby(['case_id', 'sensor_name'])['time'].agg(
        lambda x: (x.max() - x.min()).total_seconds() + 1
    ).reset_index(name='sensor_active_seconds')
    
    valid_readings = valid_df.groupby(['case_id', 'sensor_name']).size().reset_index(name='valid_readings')
    
    # Merge and calculate percentages
    uptime_df = pd.merge(valid_readings, sensor_window, on=['case_id', 'sensor_name'])
    uptime_df['uptime_percentage'] = (uptime_df['valid_readings'] / uptime_df['sensor_active_seconds'] * 100).clip(upper=100.0)
    uptime_df['null_rate_percentage'] = 100.0 - uptime_df['uptime_percentage']
    
    return uptime_df[uptime_df['uptime_percentage'] > 0]

def _detect_signal_anomalies(long_df: pd.DataFrame, active_sensors_df: pd.DataFrame) -> pd.DataFrame:
    """Detects frozen signals and noise spikes."""
    active_mask = long_df.set_index(['case_id', 'sensor_name']).index.isin(active_sensors_df.set_index(['case_id', 'sensor_name']).index)
    active_df = long_df[active_mask].copy()

    # Time sorting is critical for sequential comparison
    active_df.sort_values(by=['case_id', 'sensor_name', 'time'], inplace=True)
    active_df['prev_reading'] = active_df.groupby(['case_id', 'sensor_name'])['reading'].shift(1)
    
    # Detect anomalies
    active_df['is_frozen'] = (active_df['reading'] == active_df['prev_reading']) & active_df['reading'].notna()
    active_df['pct_change'] = (active_df['reading'] - active_df['prev_reading']).abs() / (active_df['prev_reading'].abs() + 1e-9)
    active_df['is_spike'] = (active_df['pct_change'] > 0.20) & active_df['reading'].notna()
    
    return active_df.groupby(['case_id', 'sensor_name']).agg(
        frozen_signal_count=('is_frozen', 'sum'),
        noise_spikes_count=('is_spike', 'sum')
    ).reset_index()

def _count_hardware_failures(quarantine_df: pd.DataFrame) -> pd.DataFrame:
    """Counts physical hardware failures (ignoring user disconnects)."""
    if quarantine_df.empty:
        return pd.DataFrame(columns=['case_id', 'sensor_name', 'hardware_failures'])
        
    real_errors = quarantine_df[~quarantine_df['error_reason'].str.contains('Out of bounds', na=False, case=False)]
    return real_errors.groupby(['case_id', 'sensor_name']).size().reset_index(name='hardware_failures')

def _enrich_with_metadata(health_df: pd.DataFrame, metadata_df: pd.DataFrame) -> pd.DataFrame:
    """Adds equipment brand information."""
    health_df = health_df.merge(
        metadata_df[['sensor_id', 'equipment_brand']], 
        left_on='sensor_name', right_on='sensor_id', how='left'
    )
    health_df['equipment_brand'] = health_df['equipment_brand'].fillna(
        health_df['sensor_name'].apply(lambda x: 'BIS' if str(x).split('_')[0].lower() == 'bis' else str(x).split('_')[0].capitalize())
    )
    return health_df

def _assign_health_grade(health_df: pd.DataFrame) -> pd.DataFrame:
    """Assigns the final grade using dynamic rules from the JSON configuration."""
    # Calculate rates
    health_df['hw_failure_rate_percentage'] = (health_df['hardware_failures'] / (health_df['sensor_active_seconds'] + 1e-9)) * 100
    health_df['spike_rate_percentage'] = (health_df['noise_spikes_count'] / health_df['valid_readings']) * 100
    health_df['frozen_rate_percentage'] = (health_df['frozen_signal_count'] / health_df['valid_readings']) * 100
    
    # Load dynamic rules
    with open(SETTINGS.sensor_health_rules_path, 'r', encoding='utf-8') as f:
        rules = json.load(f)
        
    sensor_names = health_df['sensor_name'].astype(str).str.lower()
    std = rules.get("standard", {})
    
    # Initialize thresholds
    max_nulls = pd.Series(std.get("max_nulls", 15.0), index=health_df.index)
    max_frozen = pd.Series(std.get("max_frozen_rate", 10.0), index=health_df.index)
    max_spikes = pd.Series(std.get("max_spike_rate", 5.0), index=health_df.index)
    
    # Apply regex rules
    for category, config in rules.items():
        if category == "standard": continue
        mask = sensor_names.str.contains(config['pattern'], na=False)
        max_nulls = np.where(mask, config['max_nulls'], max_nulls)
        max_frozen = np.where(mask, config['max_frozen_rate'], max_frozen)
        max_spikes = np.where(mask, config['max_spike_rate'], max_spikes)

    # Apply grading logic
    conditions = [
        (health_df['hw_failure_rate_percentage'] > 5.0) | (health_df['null_rate_percentage'] > max_nulls),
        (health_df['null_rate_percentage'] > (max_nulls * 0.7)) | (health_df['frozen_rate_percentage'] > max_frozen) | 
        (health_df['spike_rate_percentage'] > max_spikes) | (health_df['hw_failure_rate_percentage'] > 1.0),
        (health_df['hardware_failures'] > 0) | (health_df['null_rate_percentage'] > 2.0)
    ]
    
    health_df['health_grade'] = np.select(conditions, ['Falha Crítica', 'Degradado', 'Aceitável'], default='Excelente')
    return health_df


@task(container_image=image_spec_gold)
def read_silver_tables() -> Tuple[pa.Table, pa.Table, pa.Table]:
    """Reads the necessary tables from the Silver layer."""
    catalog = load_catalog("hive_prod", **SETTINGS.catalog_config_silver)
    
    clean_table = catalog.load_table(SILVER_CLEAN_ID).scan().to_arrow()
    quarantine_table = catalog.load_table(SILVER_QUARANTINE_ID).scan().to_arrow()
    metadata_table = catalog.load_table(SILVER_METADATA_ID).scan().to_arrow()
    
    return clean_table, quarantine_table, metadata_table

@task(container_image=image_spec_gold)
def execute_health_scoring_pipeline(clean_pa: pa.Table, quarantine_pa: pa.Table, metadata_pa: pa.Table) -> pa.Table:
    """
    Executes the analytical pipeline to grade the sensors.
    """
    clean_df = clean_pa.to_pandas()
    quarantine_df = quarantine_pa.to_pandas()
    metadata_df = metadata_pa.to_pandas()
    
    final_cols = ['case_id', 'sensor_name', 'equipment_brand', 'null_rate_percentage', 
                  'frozen_signal_count', 'noise_spikes_count', 'hardware_failures', 'health_grade']
    
    if clean_df.empty:
        return pa.Table.from_pandas(pd.DataFrame(columns=final_cols))

    # --- PIPELINE DE PROCESSAMENTO ---
    long_data = _unpivot_vitals_data(clean_df)
    health_df = _calculate_uptime_metrics(long_data)
    
    anomalies = _detect_signal_anomalies(long_data, health_df)
    health_df = health_df.merge(anomalies, on=['case_id', 'sensor_name'], how='left')
    
    hw_failures = _count_hardware_failures(quarantine_df)
    health_df = health_df.merge(hw_failures, on=['case_id', 'sensor_name'], how='left')
    health_df['hardware_failures'] = health_df['hardware_failures'].fillna(0).astype(int)
    
    health_df = _enrich_with_metadata(health_df, metadata_df)

    health_df = _assign_health_grade(health_df)

    return pa.Table.from_pandas(health_df[final_cols])

@workflow
def workflow_sensor_health_summary() -> bool:
    clean_data, quarantine_data, metadata_data = read_silver_tables()
    
    sensor_health = execute_health_scoring_pipeline(
        clean_pa=clean_data,
        quarantine_pa=quarantine_data,
        metadata_pa=metadata_data
    )

    validated_sensor_health = quality_gate(
        data_pa=sensor_health, 
        contract_path=SETTINGS.contract_vitals_sensor_health_gold
    )
    
    status_uptime = write_dataframe_to_iceberg(
        data=validated_sensor_health, 
        namespace=SETTINGS.namespace_gold, 
        table_name=SETTINGS.table_gold_sensor_health_summary,
        catalog_config=SETTINGS.catalog_config_gold,
        mode="overwrite"
    )

    return status_uptime