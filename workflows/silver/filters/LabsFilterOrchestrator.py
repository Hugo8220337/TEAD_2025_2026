import pandas as pd
import numpy as np

class LabFilterOrchestrator:
    """
    Filtra ruído laboratorial, classifica a série temporal em fases cirúrgicas,
    e injeta os metadados de controlo laboratorial.
    """
    def __init__(self):
        self.metadata_records = [
            {"name": "hb", "min_normal": 13.0, "max_normal": 17.0, "unit": "g/dL"},
            {"name": "plt", "min_normal": 130.0, "max_normal": 400.0, "unit": "x1000/mcL"},
            {"name": "wbc", "min_normal": 4.0, "max_normal": 10.0, "unit": "x1000/mcL"},
            {"name": "gluc", "min_normal": 70.0, "max_normal": 110.0, "unit": "mg/dL"},
            {"name": "cr", "min_normal": 0.70, "max_normal": 1.40, "unit": "mg/dL"}
        ]

    def apply_filters(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        # 1. Limpeza de colunas e tipos 
        df.columns = [str(c).strip().lower() for c in df.columns]
        df["result"] = pd.to_numeric(df["result"], errors="coerce")
        df["dt"] = pd.to_numeric(df["dt"], errors="coerce")

        # 2. Regras de Integridade
        mask_valid = df["caseid"].notna() & df["result"].notna() & df["dt"].notna() & (df["result"] >= 0)
        
        clean_df = df[mask_valid].copy()
        quarantine_df = df[~mask_valid].copy()  
        quarantine_df["error_reason"] = "Valor laboratorial nulo ou negativo"
        
        # 3. Classificação e Flag de Recência
        clean_df["surgery_phase"] = np.where(clean_df["dt"] < 0, "PRE_OP", "POS_OP")
        
        clean_df = clean_df.sort_values(by=['caseid', 'name', 'surgery_phase', 'dt'])
        clean_df['is_most_recent'] = ~clean_df.duplicated(subset=['caseid', 'name', 'surgery_phase'], keep='last')

        metadata_df = pd.DataFrame(self.metadata_records)

        if quarantine_df.empty:
            quarantine_df = pd.DataFrame(columns=clean_df.columns.tolist() + ["error_reason"])

        return clean_df, metadata_df, quarantine_df