import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from flytekit import ImageSpec, task
import s3fs

from utils.config import SETTINGS
from utils.contract_validator import ContractValidator

image_spec_shared = ImageSpec(
    name="sensor_quality_and_anomalies_shared",
    registry="localhost:30000",
    packages=[
        "flytekit==1.10.0",
        "pyarrow>=10.0.1",
        "pyiceberg>=0.6.0",
        "s3fs>=2023.12.2",
    ],
)

def _ensure_s3_infrastructure(namespace: str, catalog_name: str, catalog_config: dict):
    """Garante que o bucket e o namespace existem antes de escrevermos dados."""
    fs = s3fs.S3FileSystem(
        key=SETTINGS.s3_access_key,
        secret=SETTINGS.s3_secret_key,
        client_kwargs={'endpoint_url': f"http://{SETTINGS.s3_endpoint}"}
    )
    if not fs.exists(namespace):
        fs.mkdir(namespace)

    catalog = load_catalog(catalog_name, **catalog_config)
    try:
        catalog.create_namespace(
            namespace,
            properties={"location": f"s3a://{namespace}/{namespace}"} 
        )
    except NamespaceAlreadyExistsError:
        pass
    
    return catalog

@task
def quality_gate(data_pa: pa.Table, contract_path: str) -> pa.Table:
    df = data_pa.to_pandas()
    validator = ContractValidator(contract_path)
    validator.validate(df)
    return data_pa

@task(container_image=image_spec_shared)
def write_dataframe_to_iceberg(
    data: pa.Table, 
    namespace: str, 
    table_name: str, 
    catalog_config: dict,
    mode: str = "append"
) -> bool:
    # Como estamos dentro de uma task, "data" é real (não é uma promessa)
    if data is None or data.num_rows == 0:
        print(f"Warning: Empty table in {namespace}.{table_name}. Ignored.")
        return True

    identifier = f"{namespace}.{table_name}"
    
    # Catalog must be created after ensuring S3 infrastructure
    catalog = _ensure_s3_infrastructure(namespace, SETTINGS.catalog_name, catalog_config)

    if mode == "overwrite":
        try:
            catalog.drop_table(identifier)
            print(f"Tabela {identifier} apagada para overwrite.")
        except NoSuchTableError:
            pass 

    try:
        table = catalog.load_table(identifier)
        table.append(data)
    except NoSuchTableError:
        table = catalog.create_table(identifier, schema=data.schema)
        table.append(data)

    return True