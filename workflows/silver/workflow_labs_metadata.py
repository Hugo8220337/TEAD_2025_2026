import os
import sys
import pandas as pd
import pyarrow as pa
from flytekit import ImageSpec, task, workflow

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from utils.config import SETTINGS
from utils.flyte_utils import quality_gate, write_dataframe_to_iceberg
from workflows.silver.filters.LabsFilterOrchestrator import LabFilterOrchestrator 

image_spec_silver_labs = ImageSpec(
    name="labs_silver",
    registry="localhost:30000",
    packages=[
        "flytekit==1.10.0", "pandas>=2.0.0", "pyarrow>=10.0.1", 
        "pyiceberg>=0.6.0", "s3fs>=2023.12.2", "numpy<2.0.0"
    ],
)

@task(container_image=image_spec_silver_labs)
def generate_labs_metadata() -> pa.Table:
    """Gera a tabela de limites biológicos diretamente a partir do Orquestrador."""
    orchestrator = LabFilterOrchestrator()
    metadata_df = pd.DataFrame(orchestrator.metadata_records)
    
    return pa.Table.from_pandas(metadata_df)

@workflow
def workflow_labs_metadata() -> bool:
    metadata_data = generate_labs_metadata()

    validated_data = quality_gate(
        data_pa=metadata_data,
        contract_path=SETTINGS.contract_labs_metadata_silver
    )

    # Escrever Tabela de Metadados
    write_status = write_dataframe_to_iceberg(
        data=validated_data, 
        namespace=SETTINGS.namespace_silver, 
        table_name=SETTINGS.table_labs_silver_metadata,
        catalog_config=SETTINGS.catalog_config_silver,
        mode="overwrite"
    )
    return write_status