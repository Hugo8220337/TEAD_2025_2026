import os
import sys
import pandas as pd
import pyarrow as pa
from typing import Tuple
from pyiceberg.catalog import load_catalog
from flytekit import ImageSpec, task, workflow

# Access parent folder to import utils
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from utils.config import SETTINGS
from utils.flyte_utils import quality_gate, write_dataframe_to_iceberg

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

@task(container_image=image_spec_gold)
def get_quarantine_data() -> pa.Table:
    catalog = load_catalog("hive_prod", **SETTINGS.catalog_config_silver)
    quarantine_table = catalog.load_table(SILVER_QUARANTINE_ID).scan().to_arrow()
    return quarantine_table

@task(container_image=image_spec_gold)
def get_metadata_data() -> pa.Table:
    catalog = load_catalog("hive_prod", **SETTINGS.catalog_config_silver)
    metadata_table = catalog.load_table(SILVER_METADATA_ID).scan().to_arrow()
    return metadata_table

@task(container_image=image_spec_gold)
def build_hardware_failure_summary(quarantine_pa: pa.Table, metadata_pa: pa.Table) -> pa.Table:
    """
    Produces Hardware Failures Summary.
    Aggregates the quarantine logs to count how many times a sensor failed per case.
    """
    quarantine_df = quarantine_pa.to_pandas()
    metadata_df = metadata_pa.to_pandas()
    
    if quarantine_df.empty:
        # Return empty schema if no errors occurred
        schema = pd.DataFrame(columns=['case_id', 'sensor_name', 'equipment_brand', 'error_reason', 'failure_count'])
        return pa.Table.from_pandas(schema)

    # Count errors per case, sensor, and reason
    summary_df = quarantine_df.groupby(['case_id', 'sensor_name', 'error_reason']).size().reset_index(name='failure_count')
    
    # Enrich with equipment brand from metadata
    summary_df = summary_df.merge(
        metadata_df[['sensor_id', 'equipment_brand']], 
        left_on='sensor_name', 
        right_on='sensor_id', 
        how='left'
    ).drop(columns=['sensor_id'])

    # Fallback if metadata is missing: try to infer brand from sensor name 
    summary_df['equipment_brand'] = summary_df['equipment_brand'].fillna(
        summary_df['sensor_name'].apply(lambda x: 'BIS' if str(x).split('_')[0].lower() == 'bis' else str(x).split('_')[0].capitalize())
    )
    
    return pa.Table.from_pandas(summary_df)

@workflow
def workflow_hardware_failures_summary() -> bool:
    quarantine_data = get_quarantine_data()
    metadata_data = get_metadata_data()
    
    hardware_failures = build_hardware_failure_summary(quarantine_pa=quarantine_data, metadata_pa=metadata_data)

    validated_hardware_failures = quality_gate(
        data_pa=hardware_failures, 
        contract_path=SETTINGS.contract_vitals_hardware_failures_gold
    )

    status_failures = write_dataframe_to_iceberg(
        data=validated_hardware_failures, 
        namespace=SETTINGS.namespace_gold, 
        table_name=SETTINGS.table_gold_hardware_failures_summary,
        catalog_config=SETTINGS.catalog_config_gold,
        mode="overwrite"
    )
    
    return status_failures