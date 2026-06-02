import os
import sys
from typing import Tuple, Optional

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from flytekit import ImageSpec, task, workflow

# garantir acesso a utils
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from utils.config import SETTINGS
from utils.flyte_utils import write_dataframe_to_iceberg

image_spec_gold = ImageSpec(
    name="sensor_quality_and_anomalies_gold",
    registry="localhost:30000",
    packages=[
        "flytekit==1.10.0",
        "pandas>=2.0.0",
        "pyarrow>=10.0.1",
        "pyiceberg>=0.6.0",
        "s3fs>=2023.12.2",
    ],
)


def _pick_column(df: pd.DataFrame, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

# task para converter tabela silver "wide" (colunas por sensor) para formato "long"

@task(container_image=image_spec_gold)
def wide_to_long(arrow_table: pa.Table) -> pa.Table:
    # Comentário: converte Arrow -> pandas
    df = arrow_table.to_pandas()
    # Comentário: identifica coluna de identificação da cirurgia/paciente e coluna de tempo
    id_col = _pick_column(df, ["surgery_id", "case_id", "case", "operation_id", "caseid"])
    time_col = _pick_column(df, ["ts", "time", "timestamp", "date", "charttime"])
    # Comentário: valida presença das colunas essenciais
    if id_col is None or time_col is None:
        raise RuntimeError(f"Não encontrou colunas id/time na silver. Colunas presentes: {list(df.columns)}")
    
    # Comentário: determina colunas de sensor (todas as restantes, excluindo id e time)
    sensor_cols = [c for c in df.columns if c not in (id_col, time_col)]
    
    # Comentário: melt para formato long (transforma cada coluna de sensor numa linha)
    long_df = df.melt(id_vars=[id_col, time_col], value_vars=sensor_cols, var_name="sensor_id", value_name="value")
    
    # Comentário: converte coluna value para numérica (força conversão, NaN para valores inválidos)
    long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")
    
    # Comentário: remove linhas com valores nulos ou inválidos
    long_df = long_df.dropna(subset=["value"])
    
    # Comentário: garante que há linhas após limpeza
    if long_df.empty:
        raise RuntimeError("Nenhum valor válido encontrado após limpeza. Verifique os dados de entrada.")
    
    # Comentário: padroniza nomes de colunas para os candidatos usados nas tasks seguintes
    long_df = long_df.rename(columns={id_col: "surgery_id", time_col: "ts"})
    
    # Comentário: garante tipo datetime para ts
    long_df["ts"] = pd.to_datetime(long_df["ts"], errors="coerce")
    
    # Comentário: remove linhas com ts inválido
    long_df = long_df.dropna(subset=["ts"])
    
    # Comentário: devolve Arrow Table com conversão segura
    return pa.Table.from_pandas(long_df, safe=False)


@task(container_image=image_spec_gold)
def read_silver_data(table_identifier: str) -> pa.Table:
    try:
        # tenta ler a tabela silver do catálogo Iceberg/Hive
        catalog = load_catalog(SETTINGS.catalog_name, **(SETTINGS.catalog_config_silver or {}))
        return catalog.load_table(table_identifier).scan().to_arrow()
    except Exception as e:
        # se falhar (ex.: metastore/Hive não disponível ou tabela não existe) usa csv local de amostra
        print(f"Warning: could not load silver table '{table_identifier}': {e}. Falling back to local CSV sample.")
        csv_path = os.path.join(project_root, "datasets", "vitals", "raw_vitals_sample.csv")
        # verifica existência do ficheiro de fallback
        if not os.path.exists(csv_path):
            raise RuntimeError(f"Fallback CSV not found at '{csv_path}'. Popule a tabela silver ou coloque o CSV nesse caminho.") from e
        # tenta ler o CSV e dá mensagens claras em caso de erro
        try:
            df = pd.read_csv(csv_path)
        except pd.errors.EmptyDataError:
            # lê alguns bytes para ajudar a diagnosticar ficheiro corrupto / sem header
            with open(csv_path, "rb") as f:
                sample = f.read(2048)
            raise RuntimeError(f"Fallback CSV at '{csv_path}' está vazio ou malformado. Preview bytes: {sample!r}") from None
        except Exception as exc:
            raise RuntimeError(f"Erro ao ler CSV de fallback '{csv_path}': {exc}") from None
        # valida conteúdo lido
        if df is None or df.empty or len(df.columns) == 0:
            raise RuntimeError(f"Fallback CSV em '{csv_path}' não contém colunas/linhas. Verifique o ficheiro.")
        # debug: mostrar colunas e primeiras linhas
        print(f"Fallback CSV loaded from '{csv_path}'. columns={list(df.columns)}; rows={len(df)}")
        # normaliza nomes comuns de coluna para 'ts'
        if "timestamp" in df.columns and "ts" not in df.columns:
            df = df.rename(columns={"timestamp": "ts"})
        if "time" in df.columns and "ts" not in df.columns:
            df = df.rename(columns={"time": "ts"})
        # garante dtype datetime
        if "ts" in df.columns:
            df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
        return pa.Table.from_pandas(df)


@task(container_image=image_spec_gold)
def compute_sensor_uptime(arrow_table: pa.Table, gap_seconds: int = 60) -> pa.Table:
    df = arrow_table.to_pandas()
    surgery_col = _pick_column(df, ["surgery_id", "case_id", "case", "operation_id"])
    time_col = _pick_column(df, ["ts", "time", "timestamp", "date"])
    sensor_col = _pick_column(df, ["sensor_id", "sensor_name", "sensor"])
    if time_col is None or surgery_col is None or sensor_col is None:
        raise RuntimeError("Colunas necessárias (surgery/time/sensor) não encontradas na tabela silver")

    df[time_col] = pd.to_datetime(df[time_col])
    df = df.sort_values([surgery_col, sensor_col, time_col])

    df["prev_ts"] = df.groupby([surgery_col, sensor_col])[time_col].shift(1)
    df["gap_secs"] = (df[time_col] - df["prev_ts"]).dt.total_seconds()
    df["new_seg"] = (df["prev_ts"].isna()) | (df["gap_secs"] > gap_seconds)
    df["seg_id"] = df.groupby([surgery_col, sensor_col])["new_seg"].cumsum()

    seg = df.groupby([surgery_col, sensor_col, "seg_id"]).agg(
        seg_start=(time_col, "first"),
        seg_end=(time_col, "last"),
    ).reset_index()
    seg["seg_seconds"] = (seg["seg_end"] - seg["seg_start"]).dt.total_seconds().clip(lower=0)

    uptime = seg.groupby([surgery_col, sensor_col])["seg_seconds"].sum().reset_index().rename(
        columns={"seg_seconds": "uptime_seconds"}
    )

    surgery_span = df.groupby(surgery_col).agg(surgery_start=(time_col, "min"), surgery_end=(time_col, "max")).reset_index()
    surgery_span["surgery_seconds"] = (surgery_span["surgery_end"] - surgery_span["surgery_start"]).dt.total_seconds().replace(0, pd.NA)

    result = uptime.merge(surgery_span[[surgery_col, "surgery_seconds"]], on=surgery_col, how="left")
    result["uptime_rate"] = result["uptime_seconds"] / result["surgery_seconds"]

    return pa.Table.from_pandas(result)


@task(container_image=image_spec_gold)
def compute_view_window(arrow_table: pa.Table, window_minutes: int = 5, low_threshold: Optional[float] = None, high_threshold: Optional[float] = None) -> pa.Table:
    df = arrow_table.to_pandas()
    surgery_col = _pick_column(df, ["surgery_id", "case_id", "case", "operation_id"])
    time_col = _pick_column(df, ["ts", "time", "timestamp", "date"])
    sensor_col = _pick_column(df, ["sensor_id", "sensor_name", "sensor"])
    value_col = _pick_column(df, ["value", "measurement", "val"])
    if time_col is None or surgery_col is None or sensor_col is None or value_col is None:
        raise RuntimeError("Colunas necessárias (surgery/time/sensor/value) não encontradas na tabela silver")

    df[time_col] = pd.to_datetime(df[time_col])
    df = df.sort_values([surgery_col, sensor_col, time_col])
    df["window_start"] = df[time_col].dt.floor(f"{window_minutes}min")

    agg = df.groupby([surgery_col, sensor_col, "window_start"]).agg(
        avg_value=(value_col, "mean"),
        stddev_value=(value_col, "std"),
        n_samples=(value_col, "count"),
    ).reset_index()

    if low_threshold is not None or high_threshold is not None:
        cond = pd.Series(False, index=df.index)
        if low_threshold is not None:
            cond = cond | (df[value_col] < low_threshold)
        if high_threshold is not None:
            cond = cond | (df[value_col] > high_threshold)
        df["is_critical"] = cond.astype(int)
        crit = df.groupby([surgery_col, sensor_col, "window_start"])["is_critical"].sum().reset_index().rename(
            columns={"is_critical": "n_critical"}
        )
        agg = agg.merge(crit, on=[surgery_col, sensor_col, "window_start"], how="left")
        agg["pct_critical"] = agg["n_critical"] / agg["n_samples"]

    return pa.Table.from_pandas(agg)


@workflow
def workflow_vitals_gold_layer(
    silver_table_identifier: str = f"{SETTINGS.namespace_silver}.{SETTINGS.table_vitals_silver_clean}",
    gold_uptime_namespace: str = SETTINGS.namespace_gold,
    gold_uptime_table: str = SETTINGS.table_gold_sensor_uptime_rates,
    gold_view_namespace: str = SETTINGS.namespace_gold,
    gold_view_table: str = "view_window",
    gap_seconds: int = 60,
    window_minutes: int = 5,
    low_threshold: Optional[float] = None,
    high_threshold: Optional[float] = None,
) -> Tuple[bool, bool]:
    silver_arrow = read_silver_data(table_identifier=silver_table_identifier)

    # normalizar wide -> long se necessário
    long_arrow = wide_to_long(arrow_table=silver_arrow)

    uptime_arrow = compute_sensor_uptime(arrow_table=long_arrow, gap_seconds=gap_seconds)
    write_uptime_status = write_dataframe_to_iceberg(
        data=uptime_arrow,
        namespace=gold_uptime_namespace,
        table_name=gold_uptime_table,
        catalog_config=SETTINGS.catalog_config_gold,
        mode="overwrite",
    )

    view_arrow = compute_view_window(arrow_table=long_arrow, window_minutes=window_minutes, low_threshold=low_threshold, high_threshold=high_threshold)
    write_view_status = write_dataframe_to_iceberg(
        data=view_arrow,
        namespace=gold_view_namespace,
        table_name=gold_view_table,
        catalog_config=SETTINGS.catalog_config_gold,
        mode="overwrite",
    )

    return write_uptime_status, write_view_status
