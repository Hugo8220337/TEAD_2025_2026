import pandas as pd
from pyiceberg.catalog import load_catalog
from utils.config import SETTINGS

def load_gold_data():
    catalog = load_catalog("hive_prod", **SETTINGS.catalog_config_gold)
    table = catalog.load_table(f"{SETTINGS.namespace_gold}.{SETTINGS.table_gold_pre_vs_pos_operation_summary}")
    return table.scan().to_pandas()

def get_filtered_data(df, dept, exame):
    df_f = df.copy()
    if dept != "Todos": df_f = df_f[df_f['department'] == dept]
    if exame != "Todos": df_f = df_f[df_f['lab_test'] == exame]
    return df_f

def get_kpis(df):
    return {
        'pacientes': df['caseid'].nunique(),
        'idade': round(df['age'].mean(), 1) if not df.empty else 0,
        'uci': round(df['icu_days'].mean(), 1) if not df.empty else 0,
        'exames': df['lab_test'].nunique()
    }