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

from utils.sensor_metadata import SENSOR_METADATA_LIST
from utils.config import SETTINGS
from utils.flyte_utils import quality_gate, write_dataframe_to_iceberg

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
def generate_sensors_metadata() -> pa.Table:
    """Generate the iceberg table with the mapping of sensors."""
    df = pd.DataFrame(SENSOR_METADATA_LIST)
    return pa.Table.from_pandas(df)

@workflow
def workflow_sensors_metadata() -> bool:
    metadata_data = generate_sensors_metadata()

    validated_metadata = quality_gate(
        data_pa=metadata_data, 
        contract_path=SETTINGS.contract_vitals_sensors_metadata_silver
    )

    write_metadata_status = write_dataframe_to_iceberg(
        data=validated_metadata, 
        namespace=SETTINGS.namespace_silver, 
        table_name=SETTINGS.table_vitals_silver_metadata,
        catalog_config=SETTINGS.catalog_config_silver,
        mode="overwrite"
    )
    
    return write_metadata_status