import os
import sys
import pyarrow as pa
from pyiceberg.catalog import load_catalog
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
def read_bronze_labs() -> pa.Table:
    """Lê os registos laboratoriais em bruto da Bronze."""
    catalog = load_catalog("hive_prod", **SETTINGS.catalog_config_bronze)
    return catalog.load_table(f"{SETTINGS.namespace_bronze}.{SETTINGS.table_labs_bronze}").scan().to_arrow()

@task(container_image=image_spec_silver_labs)
def process_labs_results(arrow_table: pa.Table) -> pa.Table:
    """Aplica os filtros e extrai apenas a tabela principal de resultados."""
    df = arrow_table.to_pandas()
    orchestrator = LabFilterOrchestrator()
    

    clean_df, _, _ = orchestrator.apply_filters(df)
    
    return pa.Table.from_pandas(clean_df)

@workflow
def workflow_labs_results() -> bool:
    raw_data = read_bronze_labs()
    clean_data = process_labs_results(arrow_table=raw_data)

    validated_data = quality_gate(
        data_pa=clean_data,
        contract_path=SETTINGS.contract_labs_results_silver
    )

    write_status = write_dataframe_to_iceberg(
        data=validated_data, 
        namespace=SETTINGS.namespace_silver, 
        table_name=SETTINGS.table_labs_silver_results,
        catalog_config=SETTINGS.catalog_config_silver
    )
    return write_status