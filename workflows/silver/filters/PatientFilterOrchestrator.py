import pandas as pd

class PatientFilterOrchestrator:
    """
    Aplica regras de integridade clínica aos dados do paciente e 
    divide os domínios lógicos (Demografia vs Contexto Cirúrgico).
    """
    def __init__(self):
        self.demographics_cols = ["caseid", "age", "sex", "height", "weight", "bmi", "asa"]
        self.surgery_cols = ["caseid", "department", "optype", "dx", "opname", "approach", "ane_type", "icu_days"]

    def apply_filters(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        # 1. Limpeza do nome das colunas
        df.columns = [str(c).strip().lower() for c in df.columns]

        # 2. Conversão segura de tipos
        cols_to_numeric = ["age", "height", "weight", "bmi", "caseid"]
        for col in cols_to_numeric:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 3. SEGURANÇA: Remover pacientes duplicados (Mantém apenas o último registo por caseid)
        df = df.drop_duplicates(subset=['caseid'], keep='last')

        # 4. Regras de Integridade Biológica
        mask_valid = (
            df["caseid"].notna() & 
            df["age"].between(0, 120, inclusive="both") &
            (df["height"].isna() | df["height"].between(40, 250)) &
            (df["weight"].isna() | df["weight"].between(2, 400))
        )

        clean_df = df[mask_valid].copy()
        quarantine_df = df[~mask_valid].copy()
        quarantine_df["error_reason"] = "Erro biológico (idade/peso/altura) ou ID inválido"

        if "sex" in clean_df.columns:
            clean_df["sex"] = clean_df["sex"].astype(str).str.upper().str.strip()

        # 5. Separação de Contextos
        demographics_df = clean_df[[c for c in self.demographics_cols if c in clean_df.columns]]
        surgery_df = clean_df[[c for c in self.surgery_cols if c in clean_df.columns]]

        if quarantine_df.empty:
            quarantine_df = pd.DataFrame(columns=df.columns.tolist() + ["error_reason"])

        return demographics_df, surgery_df, quarantine_df