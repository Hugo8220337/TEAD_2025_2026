import os
import sys
import datetime
import pyarrow as pa
import pyarrow.csv as pv
from flytekit import task, workflow
from flytekit import ImageSpec

# Access parent folder to import utils
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from utils.config import SETTINGS
from utils.flyte_utils import quality_gate, write_dataframe_to_iceberg


image_spec = ImageSpec(
    name="sensor_quality_and_anomalies_bronze",
    registry="localhost:30000",
    packages=[
        "flytekit==1.10.0",
        "pyarrow>=10.0.1",
        "pyiceberg>=0.6.0",
        "s3fs>=2023.12.2",
        "numpy<2.0.0",
        "pandas<2.0.0" # tenho dúvidas quanto a isto
    ],
)

def _force_nulls_to_string(arrow_table):
    """Detect columns inferred as NULL and convert them to STRING type."""
    for i, field in enumerate(arrow_table.schema):
        if pa.types.is_null(field.type):
            print(f"Warning: Column '{field.name}' detected as NULL. Forcing to STRING.")
            arrow_table = arrow_table.set_column(
                i, 
                pa.field(field.name, pa.string()), 
                pa.array([None] * arrow_table.num_rows, type=pa.string())
            )
            
    return arrow_table
        
@task(container_image=image_spec)
def extract_and_format_csv(csv_path: str) -> pa.Table:
    """Read the local CSV and format the data to ensure compatibility with Iceberg."""
    arrow_table = pv.read_csv(csv_path)

    # Search for columns whose type was inferred as NULL and force them to STRING
    arrow_table = _force_nulls_to_string(arrow_table)

    # add injest time (UTC)
    arrow_table = arrow_table.append_column(
        "ingest_time", 
        pa.array([datetime.datetime.now()] * arrow_table.num_rows, type=pa.timestamp("us"))
    )

    # add source_system collumn (VitalDB)
    arrow_table = arrow_table.append_column(
        "source_system",
        pa.array(["VitalDB"] * arrow_table.num_rows, type=pa.string())
    )

    return arrow_table

@workflow
def workflow_vitals_bronze_layer(csv_path: str) -> bool:
    formatted_data = extract_and_format_csv(csv_path=csv_path)

    validated_data = quality_gate(
        data_pa=formatted_data, 
        contract_path=SETTINGS.contract_vitals_bronze
    )
    
    write_status = write_dataframe_to_iceberg(
        data=validated_data, 
        namespace=SETTINGS.namespace_bronze, 
        table_name=SETTINGS.table_vitals_bronze, 
        catalog_config=SETTINGS.catalog_config_bronze,
        mode="append"
    )
    
    return write_status