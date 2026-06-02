import os
import sys
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from flytekit import ImageSpec, task, workflow

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from utils.config import SETTINGS
from utils.flyte_utils import quality_gate, write_dataframe_to_iceberg

image_spec_gold = ImageSpec(
    name="gold_layer",
    registry="localhost:30000",
    packages=[
        "flytekit==1.10.0", "pandas>=2.0.0", "pyarrow>=10.0.1", 
        "pyiceberg>=0.6.0", "s3fs>=2023.12.2"
    ],
)

@task(container_image=image_spec_gold)
def load_silver_tables() -> tuple[pa.Table, pa.Table, pa.Table]:
    """Carrega as tabelas Silver necessárias para a análise."""
    catalog = load_catalog("hive_prod", **SETTINGS.catalog_config_silver)
    
    labs = catalog.load_table(f"{SETTINGS.namespace_silver}.{SETTINGS.table_labs_silver_results}").scan().to_arrow()
    demographics = catalog.load_table(f"{SETTINGS.namespace_silver}.{SETTINGS.table_patients_silver_demographics}").scan().to_arrow()
    surgery = catalog.load_table(f"{SETTINGS.namespace_silver}.{SETTINGS.table_patients_silver_surgery}").scan().to_arrow()
    
    return labs, demographics, surgery

@task(container_image=image_spec_gold)
def build_pre_vs_pos_model(labs_pa: pa.Table, demo_pa: pa.Table, surg_pa: pa.Table) -> pa.Table:
    """Faz o pivot dos laboratórios e cruza com os pacientes."""
    df_labs = labs_pa.to_pandas()
    df_demo = demo_pa.to_pandas()
    df_surg = surg_pa.to_pandas()

    # 1. Calcular a média dos exames por Paciente, Teste e Fase (PRE vs POS)
    labs_avg = df_labs.groupby(['caseid', 'name', 'surgery_phase'])['result'].mean().reset_index()

    # 2. Fazer o PIVOT: Passar o PRE_OP e POS_OP de linhas para colunas
    labs_pivot = labs_avg.pivot(index=['caseid', 'name'], columns='surgery_phase', values='result').reset_index()
    
    # Limpar o nome das colunas após o pivot
    labs_pivot.columns.name = None 
    
    # 3. Filtrar: Só nos interessam pacientes que tenham AMBAS as medições (Antes e Depois)
    if 'PRE_OP' not in labs_pivot.columns: labs_pivot['PRE_OP'] = None
    if 'POS_OP' not in labs_pivot.columns: labs_pivot['POS_OP'] = None
    
    labs_complete = labs_pivot.dropna(subset=['PRE_OP', 'POS_OP']).copy()

    # 4. Calcular os Deltas (Variação Absoluta e Percentual)
    labs_complete['absolute_delta'] = labs_complete['POS_OP'] - labs_complete['PRE_OP']
    labs_complete['percent_delta'] = (labs_complete['absolute_delta'] / labs_complete['PRE_OP']) * 100

    # 5. O Grande JOIN: Juntar com Demografia e Cirurgia
    gold_df = labs_complete.merge(df_demo, on='caseid', how='inner')
    gold_df = gold_df.merge(df_surg, on='caseid', how='inner')

    # Renomear as colunas para ficar elegante no Iceberg
    gold_df.rename(columns={'name': 'lab_test', 'PRE_OP': 'pre_op_avg', 'POS_OP': 'pos_op_avg'}, inplace=True)

    return pa.Table.from_pandas(gold_df)

@workflow
def workflow_pre_vs_pos_gold_layer() -> bool:
    # 1. Extrair da Silver
    labs_data, demo_data, surg_data = load_silver_tables()
    
    # 2. Transformar para a Gold
    gold_model = build_pre_vs_pos_model(labs_pa=labs_data, demo_pa=demo_data, surg_pa=surg_data)

    validated_model = quality_gate(
        data_pa=gold_model,
        contract_path=SETTINGS.contract_pre_vs_pos_operation_gold
    )

    # 3. Carregar na Gold
    write_status = write_dataframe_to_iceberg(
        data=validated_model, 
        namespace=SETTINGS.namespace_gold, 
        table_name=SETTINGS.table_gold_pre_vs_pos_operation_summary, # Certifica-te que está no config.py
        catalog_config=SETTINGS.catalog_config_gold,
        mode="overwrite" # Na Gold, por norma, fazemos overwrite da vista agregada
    )
    
    return write_status