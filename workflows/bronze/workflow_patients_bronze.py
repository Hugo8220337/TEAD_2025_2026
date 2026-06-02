import os
import sys
import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pv
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from flytekit import task, workflow, ImageSpec

# Configuração de caminhos
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

# Imports locais
from utils.config import SETTINGS
from utils.flyte_utils import quality_gate

# Definição da Imagem Docker
image_spec_patients = ImageSpec(
    name="patients_bronze_ingestion",
    registry="localhost:30000",
    packages=[
        "flytekit==1.10.0",
        "pyarrow>=10.0.1",
        "pyiceberg>=0.6.0",
        "s3fs>=2023.12.2",
        "numpy<2.0.0",
        "pandas" # Adicionado pandas aqui para garantir que existe no contentor
    ],
)

def _force_nulls_to_string(arrow_table):
    """
    Deteta colunas inferidas como NULL e converte-as para STRING.
    Isto é crucial em dados clínicos, pois muitas colunas (ex: comorbidades, notas)
    podem vir completamente vazias num lote inicial.
    """
    for i, field in enumerate(arrow_table.schema):
        if pa.types.is_null(field.type):
            print(f"Aviso: Coluna '{field.name}' detetada como NULL. A forçar para STRING.")
            arrow_table = arrow_table.set_column(
                i, 
                pa.field(field.name, pa.string()), 
                pa.array([None] * arrow_table.num_rows, type=pa.string())
            )
            
    return arrow_table
        
@task(container_image=image_spec_patients)
def extract_and_format_clinical_csv(csv_path: str) -> pa.Table:
    """Lê o CSV clínico local e formata os dados para o Iceberg."""
    arrow_table = pv.read_csv(csv_path)
    arrow_table = _force_nulls_to_string(arrow_table)
    
    num_rows = arrow_table.num_rows
    
    # 1. Coluna source_system
    source_array = pa.array(["Clinical_EHR"] * num_rows, type=pa.string())
    arrow_table = arrow_table.append_column("source_system", source_array)
    
    # 2. Coluna ingest_time
    now = datetime.datetime.utcnow()
    time_array = pa.array([now] * num_rows, type=pa.timestamp('us'))
    arrow_table = arrow_table.append_column("ingest_time", time_array)
    
    return arrow_table

@task(container_image=image_spec_patients)
def idempotency_check(    
    data: pa.Table, 
    namespace: str, 
    table_name: str, 
    catalog_config: dict,
    mode: str = "append"
) -> bool:
    """Faz a verificação de idempotência (Upsert) na tabela Iceberg."""
    catalog = load_catalog("default", **catalog_config)
    full_table_name = f"{namespace}.{table_name}"
    chaves_primarias = ['caseid', 'subjectid']
    
    # Adicionamos as colunas que NÃO DEVEM ser comparadas para a idempotência
    colunas_a_ignorar = chaves_primarias + ['ingest_time', 'source_system']
    
    df_novos = data.to_pandas()
    
    try:
        table = catalog.load_table(full_table_name)
        df_atuais = table.scan().to_pandas()
    except NoSuchTableError:
        print(f"Tabela {full_table_name} não encontrada. A criar nova tabela e a inserir dados.")
        catalog.create_table(full_table_name, schema=data.schema)
        table = catalog.load_table(full_table_name)
        table.append(data)
        return True

    if df_atuais.empty:
        print("Tabela existe mas está vazia. A efetuar append direto.")
        table.append(data)
        return True

    df_merge = pd.merge(df_novos, df_atuais, on=chaves_primarias, how='left', indicator=True, suffixes=('', '_atual'))

    df_inserir = df_merge[df_merge['_merge'] == 'left_only'].copy()
    df_inserir = df_inserir[df_novos.columns]

    df_existentes = df_merge[df_merge['_merge'] == 'both'].copy()
    
    colunas_valores = [col for col in df_novos.columns if col not in colunas_a_ignorar]
    tem_diferenca = pd.Series(False, index=df_existentes.index)
    
    for col in colunas_valores:
        col_nova = df_existentes[col]
        col_atual = df_existentes[f'{col}_atual']
        diferenca_col = (col_nova != col_atual) & ~(col_nova.isna() & col_atual.isna())
        tem_diferenca = tem_diferenca | diferenca_col

    df_atualizar = df_existentes[tem_diferenca].copy()
    
    # NOTA: Quando atualizamos a linha, queremos manter a nova 'ingest_time' que o ficheiro novo trouxe
    df_atualizar = df_atualizar[df_novos.columns]

    if df_inserir.empty and df_atualizar.empty:
        print("Idempotência garantida: Nenhum registo novo e nenhuma alteração detetada nos registos existentes.")
        return True

    print(f"Alterações detetadas: {len(df_inserir)} novos registos, {len(df_atualizar)} atualizações.")

    if not df_atualizar.empty:
        print("\n🔍 --- Detalhe das Alterações ---")
        # df_existentes tem as colunas novas e as antigas (com sufixo '_atual')
        df_mudancas = df_existentes[tem_diferenca]
        
        # Limitar o print a 50 linhas para não inundar os logs se houver milhares de mudanças
        limite_logs = 50 
        for i, (index, row) in enumerate(df_mudancas.iterrows()):
            if i >= limite_logs:
                print(f"... e mais {len(df_mudancas) - limite_logs} registos atualizados (ocultados dos logs).")
                break
                
            # Identificar de quem estamos a falar (as chaves)
            id_paciente = ", ".join([f"{k}: {row[k]}" for k in chaves_primarias])
            
            # Procurar as colunas específicas que mudaram
            mudancas_linha = []
            for col in colunas_valores:
                val_novo = row[col]
                val_antigo = row[f'{col}_atual']
                
                # Se for diferente E não forem ambos NaN
                if val_novo != val_antigo and not (pd.isna(val_novo) and pd.isna(val_antigo)):
                    mudancas_linha.append(f"{col} ({val_antigo} ➔ {val_novo})")
            
            if mudancas_linha:
                print(f"[{id_paciente}] Alterou: " + " | ".join(mudancas_linha))
        print("---------------------------------\n")

    df_atuais_mantidos = pd.merge(df_atuais, df_atualizar[chaves_primarias], on=chaves_primarias, how='left', indicator=True)
    df_atuais_mantidos = df_atuais_mantidos[df_atuais_mantidos['_merge'] == 'left_only'].drop(columns=['_merge'])

    df_final = pd.concat([df_atuais_mantidos, df_atualizar, df_inserir], ignore_index=True)
    
    pa_final = pa.Table.from_pandas(df_final, schema=data.schema)
    table.overwrite(pa_final)
    
    return True


@workflow
def workflow_patients_bronze_layer(csv_path: str) -> bool:
    """Orquestra a extração, validação e inserção segura dos dados na Bronze."""
    # 1. Extrair e Formatar
    formatted_data = extract_and_format_clinical_csv(csv_path=csv_path)

    # 2. Validar a Qualidade
    validated_data = quality_gate(
        data_pa=formatted_data,
        contract_path=SETTINGS.contract_patients_bronze
    )

    # 3. Escrever com Idempotência (Substitui o antigo write_patients_bronze)
    write_status = idempotency_check(
        data=validated_data, 
        namespace=SETTINGS.namespace_bronze, 
        table_name=SETTINGS.table_patients_bronze, 
        catalog_config=SETTINGS.catalog_config_bronze,
        mode="append"
    )
    
    return write_status