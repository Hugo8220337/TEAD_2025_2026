# config.py
# Configuração global do pipeline Lakehouse (VitalDB).

import os
from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class Settings:
    BASE_DIR: Path = Path(__file__).resolve().parent.parent
    CONTRACTS_DIR: Path = BASE_DIR / "contracts"
    UTILS_DIR: Path = BASE_DIR / "utils"

    # For workflow_sensor_health_summary.py
    sensor_health_rules_path: str = str(UTILS_DIR / "sensor_health_rules.json")


    # CONTRACTS - Bronze
    contract_vitals_bronze: str = str(CONTRACTS_DIR / "bronze" / "bronze.vitals.contract.yaml")
    contract_labs_bronze: str = str(CONTRACTS_DIR / "bronze" / "bronze.labs.contract.yaml")
    contract_patients_bronze: str = str(CONTRACTS_DIR / "bronze" / "bronze.patients.contract.yaml")

    # CONTRACTS - Silver
    contract_vitals_clean_silver: str = str(CONTRACTS_DIR / "silver" / "silver.vitals_clean.contract.yaml")
    contract_vitals_quarantine_silver: str = str(CONTRACTS_DIR / "silver" / "silver.vitals_quarantine.contract.yaml")
    contract_vitals_sensors_metadata_silver: str = str(CONTRACTS_DIR / "silver" / "silver.sensors_metadata.contract.yaml")
    contract_labs_results_silver: str = str(CONTRACTS_DIR / "silver" / "silver.labs_results.contract.yaml")
    contract_labs_metadata_silver: str = str(CONTRACTS_DIR / "silver" / "silver.labs_metadata.contract.yaml")
    contract_labs_quarantine_silver: str = str(CONTRACTS_DIR / "silver" / "silver.labs_quarantine.contract.yaml")
    contract_patients_demographics_silver: str = str(CONTRACTS_DIR / "silver" / "silver.patients_demographics.contract.yaml")
    contract_patients_surgery_silver: str = str(CONTRACTS_DIR / "silver" / "silver.patients_surgery.contract.yaml")
    contract_patients_quarantine_silver: str = str(CONTRACTS_DIR / "silver" / "silver.patients_quarantine.contract.yaml")

    # CONTRACTS - Gold
    contract_vitals_hardware_failures_gold: str = str(CONTRACTS_DIR / "gold" / "gold.hardware_failures_summary.contract.yaml")
    contract_vitals_sensor_health_gold: str = str(CONTRACTS_DIR / "gold" / "gold.sensor_health_summary.contract.yaml")
    contract_vitals_cross_sensor_drift: str = str(CONTRACTS_DIR / "gold" / "gold.cross_sensor_drift.contract copy.yaml")
    contract_pre_vs_pos_operation_gold: str = str(CONTRACTS_DIR / "gold" / "gold.pre_vs_pos_operation.contract.yaml")
    contract_icu_ml_features_gold: str = str(CONTRACTS_DIR / "gold" / "gold.icu_ml_features.contract.yaml")



    # ---------------------------------------------------------
    # INFRAESTRUTURA E CREDENCIAIS (Com variáveis de ambiente)
    # ---------------------------------------------------------
    s3_endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    s3_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    s3_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    s3_region: str = os.getenv("MINIO_REGION", "us-east-1")
    
    hive_uri: str = os.getenv("HIVE_METASTORE_URI", "thrift://localhost:9083")

    # ---------------------------------------------------------
    # LAKEHOUSE: NAMESPACES (Buckets) E TABELAS
    # ---------------------------------------------------------
    catalog_name: str = "hive_prod"
    
    namespace_bronze: str = os.getenv("BRONZE_BUCKET", "bronze")
    namespace_silver: str = os.getenv("SILVER_BUCKET", "silver")
    namespace_gold: str = os.getenv("GOLD_BUCKET", "gold")

    # vitals tables
    table_vitals_bronze: str = "vitals" 
    table_vitals_silver_clean: str = "vitals_clean"
    table_vitals_silver_quarantine: str = "vitals_quarantine"
    table_vitals_silver_metadata: str = "sensors_metadata"

    # labs tables
    table_labs_bronze: str = "labs"
    table_labs_silver_results: str = "labs_results_classified"
    table_labs_silver_metadata: str = "labs_metadata_limits"
    table_labs_silver_quarantine: str = "labs_quarantine"

    # patients tables
    table_patients_bronze: str = "patients"
    table_patients_silver_demographics: str = "patients_demographics"
    table_patients_silver_surgery: str = "patients_surgery_context"
    table_patients_silver_quarantine: str = "patients_quarantine"

    # Gold tables
    table_gold_hardware_failures_summary: str = "hardware_failures_summary"
    table_gold_sensor_health_summary: str = "sensor_health_summary"
    table_gold_cross_sensor_drift: str = "cross_sensor_drift"
    table_gold_pre_vs_pos_operation_summary: str = "pre_vs_post_operation"
    table_gold_sensor_uptime_rates: str = "sensor_uptime_rates"
    table_gold_view_window: str = "view_window"
    table_gold_icu_ml_features: str = "icu_ml_features"

    # ---------------------------------------------------------
    # CAMADA BRONZE: Parâmetros de Ingestão
    # ---------------------------------------------------------
    default_interval_sec: float = 1.0

    # ---------------------------------------------------------
    # CAMADA GOLD: Parâmetros Analíticos e Limiares (AINDA NÃO FOI USADA)
    # ---------------------------------------------------------

    # PARÂMETROS DO WORKFLOW GOLD (workflow_sensor_view_gold.py)
    gap_seconds: int = 60  # GAP MÁXIMO ENTRE AMOSTRAS PARA SEGMENTAÇÃO DE UPTIME
    window_minutes: int = 5  # TAMANHO DA JANELA TEMPORAL PARA AGREGAÇÃO
    low_threshold_vitals: float = None  # LIMIAR INFERIOR PARA CRITICIDADE (None = sem validação)
    high_threshold_vitals: float = None  # LIMIAR SUPERIOR PARA CRITICIDADE (None = sem validação)

    ma_window_rows: int = 20
    std_window_rows: int = 20
    default_sample_seconds: int = 1

    map_low_threshold: float = 65.0
    spo2_low_threshold: float = 92.0
    hr_high_threshold: float = 120.0

    # ---------------------------------------------------------
    # Métodos para a Geração dinâmica de Catálogos
    # ---------------------------------------------------------
    @property
    def catalog_config_bronze(self) -> dict:
        """Gera o dicionário de configuração do PyIceberg para a camada Bronze."""
        return {
            "type": "hive",
            "uri": self.hive_uri,
            "s3.endpoint": f"http://{self.s3_endpoint}",
            "s3.access-key-id": self.s3_access_key,
            "s3.secret-access-key": self.s3_secret_key,
            "warehouse": f"s3://{self.namespace_bronze}/",
            "s3.path-style-access": "true"
        }

    @property
    def catalog_config_silver(self) -> dict:
        """Gera o dicionário de configuração do PyIceberg para a camada Silver."""
        return {
            "type": "hive",
            "uri": self.hive_uri,
            "s3.endpoint": f"http://{self.s3_endpoint}",
            "s3.access-key-id": self.s3_access_key,
            "s3.secret-access-key": self.s3_secret_key,
            "warehouse": f"s3a://{self.namespace_silver}/", 
            "s3.path-style-access": "true"
        }
    
    @property
    def catalog_config_gold(self) -> dict:
        """Generates the PyIceberg config dictionary for the Gold layer."""
        return {
            "type": "hive",
            "uri": self.hive_uri,
            "s3.endpoint": f"http://{self.s3_endpoint}",
            "s3.access-key-id": self.s3_access_key,
            "s3.secret-access-key": self.s3_secret_key,
            "warehouse": f"s3a://{self.namespace_gold}/", 
            "s3.path-style-access": "true"
        }

# Instância global para ser importada noutros ficheiros
SETTINGS = Settings()