import os
import sys
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from flytekit import ImageSpec, task, workflow
from itertools import combinations

# Access parent folder to import utils
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from utils.config import SETTINGS
from utils.flyte_utils import quality_gate, write_dataframe_to_iceberg

SILVER_CLEAN_ID = f"{SETTINGS.namespace_silver}.{SETTINGS.table_vitals_silver_clean}"
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


@task(container_image=image_spec_gold)
def get_vitals_clean_data() -> pa.Table:
    catalog = load_catalog("hive_prod", **SETTINGS.catalog_config_silver)
    clean_table = catalog.load_table(SILVER_CLEAN_ID).scan().to_arrow()
    return clean_table

@task(container_image=image_spec_gold)
def get_metadata_data() -> pa.Table:
    catalog = load_catalog("hive_prod", **SETTINGS.catalog_config_silver)
    metadata_table = catalog.load_table(SILVER_METADATA_ID).scan().to_arrow()
    return metadata_table

@task(container_image=image_spec_gold)
def build_cross_sensor_drift(clean_pa: pa.Table, metadata_pa: pa.Table) -> pa.Table:
    """
    Produces Cross-Sensor Drift Analysis.
    Finds sensors measuring the same parameter and calculates the drift directly on wide columns.
    To avoid memory issues, it processes one parameter group at a time and only keeps necessary columns in memory.
    """
    clean_df = clean_pa.to_pandas()
    metadata_df = metadata_pa.to_pandas()
    
    if clean_df.empty or metadata_df.empty:
        return pa.Table.from_pandas(pd.DataFrame(columns=['case_id', 'time', 'parameter_measured', 'sensor_id_1', 'sensor_id_2', 'drift_value']))

    drift_records = []
    
    # Group by the parameter measured (e.g., group all 'Heart Rate' sensors together)
    for param, group in metadata_df.groupby('parameter_measured'):
        sensors = group['sensor_id'].tolist()
        
        # Obviosly it only makes sense to compare if wehave 2 or more sensors measuring the same thing
        if len(sensors) >= 2:
            # Create all possible pairs of sensors measuring the same parameter
            for s1, s2 in combinations(sensors, 2):
                
                # Make sure that the sensor columns exist in the clean data
                if s1 in clean_df.columns and s2 in clean_df.columns:
                    
                    # Filter only rows where BOTH sensors have non-null values to ensure valid drift calculation
                    valid_mask = clean_df[s1].notna() & clean_df[s2].notna()
                    
                    if valid_mask.any():
                        # extract only the relevant columns for the valid rows to save memory
                        pair_df = clean_df.loc[valid_mask, ['case_id', 'time', s1, s2]].copy()
                        
                        # Calculate absolute drift between the two sensors for the same parameter at the same time 
                        pair_df['drift_value'] = (pair_df[s1] - pair_df[s2]).abs()
                        
                        # Add identifiers
                        pair_df['parameter_measured'] = param
                        pair_df['sensor_id_1'] = s1
                        pair_df['sensor_id_2'] = s2
                        
                        # Format final columns
                        pair_df = pair_df[['case_id', 'time', 'parameter_measured', 'sensor_id_1', 'sensor_id_2', 'drift_value']]
                        drift_records.append(pair_df)
    
    # Fuse results into a single Dataframe
    if drift_records:
        final_drift_df = pd.concat(drift_records, ignore_index=True)
    else:
        final_drift_df = pd.DataFrame(columns=['case_id', 'time', 'parameter_measured', 'sensor_id_1', 'sensor_id_2', 'drift_value'])
        
    return pa.Table.from_pandas(final_drift_df)

@workflow
def workflow_cross_sensor_drift() -> bool:
    clean_data = get_vitals_clean_data()
    metadata_data = get_metadata_data()
    
    cross_drift = build_cross_sensor_drift(clean_pa=clean_data, metadata_pa=metadata_data)
    
    validated_cross_drift = quality_gate(
        data_pa=cross_drift, 
        contract_path=SETTINGS.contract_vitals_cross_sensor_drift
    )

    status_drift = write_dataframe_to_iceberg(
        data=validated_cross_drift, 
        namespace=SETTINGS.namespace_gold, 
        table_name=SETTINGS.table_gold_cross_sensor_drift,
        catalog_config=SETTINGS.catalog_config_gold,
        mode="overwrite"
    )
    
    return status_drift