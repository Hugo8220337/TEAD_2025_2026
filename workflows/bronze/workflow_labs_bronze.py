import os
import sys
import pyarrow as pa
import pyarrow.csv as pv
import datetime
from flytekit import task, workflow, ImageSpec


project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from utils.config import SETTINGS
from utils.flyte_utils import quality_gate, write_dataframe_to_iceberg

image_spec_labs = ImageSpec(
    name="labs_bronze_ingestion",
    registry="localhost:30000",
    packages=[
        "flytekit==1.10.0",
        "pyarrow>=10.0.1",
        "pyiceberg>=0.6.0",
        "s3fs>=2023.12.2",
        "numpy<2.0.0"
    ],
)

def _force_nulls_to_string(arrow_table):
    """
    Garante que colunas com valores nulos inferidos pelo PyArrow 
    sejam forçadas a STRING para não quebrar a inserção no Iceberg.
    """
    for i, field in enumerate(arrow_table.schema):
        if pa.types.is_null(field.type):
            print(f"Aviso: Coluna '{field.name}' (Labs) detetada como NULL. A forçar para STRING.")
            arrow_table = arrow_table.set_column(
                i, 
                pa.field(field.name, pa.string()), 
                pa.array([None] * arrow_table.num_rows, type=pa.string())
            )
    return arrow_table
        
@task(container_image=image_spec_labs)
def extract_and_format_lab_csv(csv_path: str) -> pa.Table:
    """Lê o ficheiro CSV de resultados laboratoriais."""
    arrow_table = pv.read_csv(csv_path)
    arrow_table = _force_nulls_to_string(arrow_table)

    num_rows = arrow_table.num_rows
    
    # 1. Coluna source_system
    source_array = pa.array(["Laboratory_Information_System"] * num_rows, type=pa.string())
    arrow_table = arrow_table.append_column("source_system", source_array)
    
    # 2. Coluna ingest_time (Timestamp em microsegundos, que o Iceberg adora)
    now = datetime.datetime.utcnow()
    time_array = pa.array([now] * num_rows, type=pa.timestamp('us'))
    arrow_table = arrow_table.append_column("ingest_time", time_array)

    return arrow_table

@workflow
def workflow_labs_bronze_layer(csv_path: str) -> bool:
    """Orquestra a ingestão da camada Bronze para Labs."""
    formatted_data = extract_and_format_lab_csv(csv_path=csv_path)

    # Aplica o gate de qualidade
    validated_data = quality_gate(
        data_pa=formatted_data, 
        contract_path=SETTINGS.contract_labs_bronze
    )

    write_status = write_dataframe_to_iceberg(
        data=validated_data, 
        namespace=SETTINGS.namespace_bronze, 
        table_name=SETTINGS.table_labs_bronze,
        catalog_config=SETTINGS.catalog_config_bronze,
        mode="append"
    )
    
    return write_status
