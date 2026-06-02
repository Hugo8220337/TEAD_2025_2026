import os
import sys
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from flytekit import ImageSpec, task, workflow

# Setup do path do projeto
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from utils.config import SETTINGS
from utils.flyte_utils import quality_gate, write_dataframe_to_iceberg
from workflows.silver.filters.PatientFilterOrchestrator import PatientFilterOrchestrator

image_spec_silver_pat = ImageSpec(
    name="patients_silver",
    registry="localhost:30000",
    packages=[
        "flytekit==1.10.0", "pandas>=2.0.0", "pyarrow>=10.0.1", 
        "pyiceberg>=0.6.0", "s3fs>=2023.12.2", "numpy<2.0.0"
    ],
)

@task(container_image=image_spec_silver_pat)
def read_bronze_patients() -> pa.Table:
    """Lê os dados clínicos em bruto da camada Bronze."""
    catalog = load_catalog("hive_prod", **SETTINGS.catalog_config_bronze)
    return catalog.load_table(f"{SETTINGS.namespace_bronze}.{SETTINGS.table_patients_bronze}").scan().to_arrow()

@task(container_image=image_spec_silver_pat)
def process_patient_demographics(arrow_table: pa.Table) -> pa.Table:
    """Aplica as regras de negócio e extrai apenas o domínio Demográfico."""
    df = arrow_table.to_pandas()
    
    orchestrator = PatientFilterOrchestrator()
    # Ignoramos a surgery_df e quarantine_df usando '_'
    demographics_df, _, _ = orchestrator.apply_filters(df)

    return pa.Table.from_pandas(demographics_df)

@workflow
def workflow_patients_demographics() -> bool:
    # 1. Extração
    raw_data = read_bronze_patients()
    
    # 2. Transformação (Apenas Demografia)
    demographics_data = process_patient_demographics(arrow_table=raw_data)

    # 2.5 Quality Gate
    validated_data = quality_gate(
        data_pa=demographics_data, 
        contract_path=SETTINGS.contract_patients_demographics_silver
    )

    # 3. Carregamento
    write_status = write_dataframe_to_iceberg(
        data=validated_data, 
        namespace=SETTINGS.namespace_silver, 
        table_name=SETTINGS.table_patients_silver_demographics, 
        catalog_config=SETTINGS.catalog_config_silver
    )
    
    return write_status