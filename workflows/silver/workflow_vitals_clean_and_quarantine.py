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

from utils.contract_validator import ContractValidator
from utils.config import SETTINGS
from utils.flyte_utils import quality_gate, write_dataframe_to_iceberg
from workflows.silver.filters.VitalsFilterOrchestrator import VitalsFilterOrchestrator

VITALS_BRONZE_IDENTIFIER = f"{SETTINGS.namespace_bronze}.{SETTINGS.table_vitals_bronze}"

image_spec_silver = ImageSpec(
    name="sensor_quality_and_anomalies_silver",
    registry="localhost:30000",
    packages=[
        "flytekit==1.10.0",
        "pandas>=2.0.0",      # os filtros não funcionam sem isto
        "pyarrow>=10.0.1",
        "pyiceberg>=0.6.0",
        "s3fs>=2023.12.2",
    ],
)
     
@task(container_image=image_spec_silver)
def read_bronze_data() -> pa.Table:
    catalog = load_catalog("hive_prod", **SETTINGS.catalog_config_bronze)
    return catalog.load_table(VITALS_BRONZE_IDENTIFIER).scan().to_arrow()


@task(container_image=image_spec_silver)
def clean_and_transform_vitals(arrow_table: pa.Table) -> Tuple[pa.Table, pa.Table]:
    df = arrow_table.to_pandas()
    
    # Normalize columns
    df.columns = [coluna.replace('/', '_').replace(' ', '_').lower() for coluna in df.columns]

    # Convert from Unix Epoch time to datetime, force (us) to avoid problems
    df['time'] = pd.to_datetime(df['time'], unit='s').astype('datetime64[us]')

    filter = VitalsFilterOrchestrator()
    clean_df, quarentine_df = filter.apply_filters(df)

    # remove errors from 'time' column in quarantine, since time is not hardware!
    if not quarentine_df.empty and 'sensor_name' in quarentine_df.columns:
        quarentine_df = quarentine_df[quarentine_df['sensor_name'] != 'time']
    
    # Handle case where quarantine DataFrame is empty (tavoid Arrow Table conversion issues)
    if quarentine_df.empty and len(quarentine_df.columns) == 0:
        quarentine_df = pd.DataFrame(columns=['case_id', 'time', 'sensor_name', 'original_value', 'error_reason'])

    # Convert both DataFrames to PyArrow Tables
    clean_arrow_table = pa.Table.from_pandas(clean_df)
    quarantine_arrow_table = pa.Table.from_pandas(quarentine_df)
    
    return clean_arrow_table, quarantine_arrow_table

@workflow
def workflow_vitals_clean_and_quarantine() -> Tuple[bool, bool]:
    raw_data = read_bronze_data()
    clean_data, quarantine_data = clean_and_transform_vitals(arrow_table=raw_data)

    # Write and validate vitals clean data
    validated_clean = quality_gate(
        data_pa=clean_data, 
        contract_path=SETTINGS.contract_vitals_clean_silver
    )
    write_clean_status = write_dataframe_to_iceberg(
        data=validated_clean, 
        namespace=SETTINGS.namespace_silver, 
        table_name=SETTINGS.table_vitals_silver_clean,
        catalog_config=SETTINGS.catalog_config_silver
    )

    # Write and validate quarantine data
    validated_quarantine = quality_gate(
        data_pa=quarantine_data, 
        contract_path=SETTINGS.contract_vitals_quarantine_silver
    )
    write_quarantine_status = write_dataframe_to_iceberg(
        data=validated_quarantine, 
        namespace=SETTINGS.namespace_silver, 
        table_name=SETTINGS.table_vitals_silver_quarantine,
        catalog_config=SETTINGS.catalog_config_silver
    )
    
    return write_clean_status, write_quarantine_status