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
    name="icu_features_gold",
    registry="localhost:30000",
    packages=[
        "flytekit==1.10.0", "pandas>=2.0.0", "pyarrow>=10.0.1",
        "pyiceberg>=0.6.0", "s3fs>=2023.12.2", "numpy<2.0.0",
    ],
)

PREOP_LABS = ["hb", "plt", "wbc", "gluc", "cr", "alb"]


@task(container_image=image_spec_gold)
def load_silver_tables() -> tuple[pa.Table, pa.Table, pa.Table]:
    catalog_silver = load_catalog("hive_prod", **SETTINGS.catalog_config_silver)
    demographics = catalog_silver.load_table(
        f"{SETTINGS.namespace_silver}.{SETTINGS.table_patients_silver_demographics}"
    ).scan().to_arrow()
    surgery = catalog_silver.load_table(
        f"{SETTINGS.namespace_silver}.{SETTINGS.table_patients_silver_surgery}"
    ).scan().to_arrow()
    labs = catalog_silver.load_table(
        f"{SETTINGS.namespace_silver}.{SETTINGS.table_labs_silver_results}"
    ).scan().to_arrow()
    return demographics, surgery, labs


@task(container_image=image_spec_gold)
def build_icu_feature_table(
    demo_pa: pa.Table,
    surg_pa: pa.Table,
    labs_pa: pa.Table,
) -> pa.Table:
    df_demo = demo_pa.to_pandas()
    df_surg = surg_pa.to_pandas()
    df_labs = labs_pa.to_pandas()

    # --- Base: demographics + surgery context (target column) ---
    features = df_demo[["caseid", "age", "sex", "bmi", "asa"]].merge(
        df_surg[["caseid", "department", "optype", "approach", "ane_type", "icu_days"]],
        on="caseid",
        how="inner",
    )
    # Only keep cases where the target is known
    features = features.dropna(subset=["icu_days"])

    # --- Pre-operative lab pivot ---
    preop = df_labs[
        (df_labs["surgery_phase"] == "PRE_OP") & (df_labs["is_most_recent"] == True)
        & (df_labs["name"].isin(PREOP_LABS))
    ].copy()
    if not preop.empty:
        labs_wide = preop.pivot_table(
            index="caseid", columns="name", values="result", aggfunc="first"
        ).reset_index()
        labs_wide.columns.name = None
        labs_wide.rename(
            columns={lab: f"preop_{lab}" for lab in PREOP_LABS if lab in labs_wide.columns},
            inplace=True,
        )
        features = features.merge(labs_wide, on="caseid", how="left")
    else:
        for lab in PREOP_LABS:
            features[f"preop_{lab}"] = float("nan")

    # Ensure float types for all numeric feature columns
    float_cols = [
        "age", "bmi", "asa",
        "preop_hb", "preop_plt", "preop_wbc", "preop_gluc", "preop_cr", "preop_alb",
        "icu_days",
    ]
    for col in float_cols:
        if col in features.columns:
            features[col] = pd.to_numeric(features[col], errors="coerce").astype("float64")

    return pa.Table.from_pandas(features, preserve_index=False)


@workflow
def icu_features_gold_workflow() -> bool:
    demo, surg, labs = load_silver_tables()

    feature_table = build_icu_feature_table(
        demo_pa=demo,
        surg_pa=surg,
        labs_pa=labs,
    )

    validated = quality_gate(
        data_pa=feature_table,
        contract_path=SETTINGS.contract_icu_ml_features_gold,
    )

    return write_dataframe_to_iceberg(
        data=validated,
        namespace=SETTINGS.namespace_gold,
        table_name=SETTINGS.table_gold_icu_ml_features,
        catalog_config=SETTINGS.catalog_config_gold,
        mode="overwrite",
    )
