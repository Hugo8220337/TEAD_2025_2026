import json
import os
import sys
import pyarrow as pa
import pandas as pd
from pyiceberg.catalog import load_catalog
from flytekit import ImageSpec, task, workflow

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from utils.config import SETTINGS

image_spec_inference = ImageSpec(
    name="icu_predictor_inference",
    registry="localhost:30000",
    packages=[
        "flytekit==1.10.0",
        "pandas>=2.0.0",
        "pyarrow>=10.0.1",
        "pyiceberg>=0.6.0",
        "s3fs>=2023.12.2",
        "numpy<2.0.0",
        "scikit-learn>=1.3.0",
        "xgboost>=2.0.0",
        "mlflow==3.10.1",
        "boto3",
    ],
)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://host.docker.internal:15000")
EXPERIMENT_REGRESSION = "ICU_Days_Regression"
EXPERIMENT_CLASSIFICATION = "ICU_Days_Classification"

NUMERIC_FEATURES = [
    "age", "bmi", "asa",
    "preop_hb", "preop_plt", "preop_wbc", "preop_gluc", "preop_cr", "preop_alb",
]
CATEGORICAL_FEATURES = ["sex", "department", "optype", "approach", "ane_type"]
PREOP_LABS = ["hb", "plt", "wbc", "gluc", "cr", "alb"]

os.environ["MLFLOW_S3_ENDPOINT_URL"] = f"http://{SETTINGS.s3_endpoint}"
os.environ["AWS_ACCESS_KEY_ID"] = SETTINGS.s3_access_key
os.environ["AWS_SECRET_ACCESS_KEY"] = SETTINGS.s3_secret_key
os.environ["AWS_DEFAULT_REGION"] = SETTINGS.s3_region
os.environ["AWS_EC2_METADATA_DISABLED"] = "true"


@task(container_image=image_spec_inference)
def fetch_case_features(caseid: str) -> pa.Table:
    catalog = load_catalog("hive_prod", **SETTINGS.catalog_config_silver)

    demo = catalog.load_table(
        f"{SETTINGS.namespace_silver}.{SETTINGS.table_patients_silver_demographics}"
    ).scan().to_arrow().to_pandas()
    surg = catalog.load_table(
        f"{SETTINGS.namespace_silver}.{SETTINGS.table_patients_silver_surgery}"
    ).scan().to_arrow().to_pandas()
    labs = catalog.load_table(
        f"{SETTINGS.namespace_silver}.{SETTINGS.table_labs_silver_results}"
    ).scan().to_arrow().to_pandas()

    demo = demo[demo["caseid"].astype(str) == caseid]
    surg = surg[surg["caseid"].astype(str) == caseid]
    labs = labs[labs["caseid"].astype(str) == caseid]

    if demo.empty or surg.empty:
        raise ValueError(f"No silver data found for caseid={caseid!r}")

    features = demo[["caseid", "age", "sex", "bmi", "asa"]].merge(
        surg[["caseid", "department", "optype", "approach", "ane_type", "icu_days"]],
        on="caseid",
        how="inner",
    )

    preop = labs[
        (labs["surgery_phase"] == "PRE_OP")
        & (labs["is_most_recent"] == True)
        & (labs["name"].isin(PREOP_LABS))
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

    float_cols = ["age", "bmi", "asa",
                  "preop_hb", "preop_plt", "preop_wbc", "preop_gluc", "preop_cr", "preop_alb",
                  "icu_days"]
    for col in float_cols:
        if col in features.columns:
            features[col] = pd.to_numeric(features[col], errors="coerce").astype("float64")

    print(f"Fetched features for caseid={caseid}: {features.shape[0]} row(s)")
    return pa.Table.from_pandas(features, preserve_index=False)


@task(container_image=image_spec_inference)
def find_best_regression_model() -> str:
    import mlflow
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    runs = mlflow.search_runs(
        experiment_names=[EXPERIMENT_REGRESSION],
        order_by=["metrics.rmse_mean ASC"],
        max_results=1,
    )
    if runs.empty:
        raise ValueError(f"No runs found in experiment '{EXPERIMENT_REGRESSION}'. Run the training workflow first.")
    best = runs.iloc[0]
    model_type = best["tags.model_type"]
    print(f"Best regression model: {model_type}  RMSE={best['metrics.rmse_mean']:.3f}")
    return f"models:/icu_{model_type}/latest"


@task(container_image=image_spec_inference)
def find_best_classification_model() -> str:
    import mlflow
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    runs = mlflow.search_runs(
        experiment_names=[EXPERIMENT_CLASSIFICATION],
        order_by=["metrics.auc_mean DESC"],
        max_results=1,
    )
    if runs.empty:
        raise ValueError(f"No runs found in experiment '{EXPERIMENT_CLASSIFICATION}'. Run the training workflow first.")
    best = runs.iloc[0]
    model_type = best["tags.model_type"]
    print(f"Best classification model: {model_type}  AUC={best['metrics.auc_mean']:.3f}")
    return f"models:/icu_{model_type}/latest"


@task(container_image=image_spec_inference)
def run_predictions(features_pa: pa.Table, reg_model_uri: str, cls_model_uri: str) -> str:
    import mlflow

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    df = features_pa.to_pandas()
    caseid = str(df["caseid"].iloc[0])
    actual_icu = (
        float(df["icu_days"].iloc[0])
        if "icu_days" in df.columns and not df["icu_days"].isna().all()
        else None
    )

    # Ensure every feature column exists; add NaN if absent so the pipeline can impute
    for col in NUMERIC_FEATURES:
        if col not in df.columns:
            df[col] = float("nan")
    for col in CATEGORICAL_FEATURES:
        if col not in df.columns:
            df[col] = None

    X = df[NUMERIC_FEATURES + CATEGORICAL_FEATURES]

    reg_model = mlflow.pyfunc.load_model(reg_model_uri)
    cls_model = mlflow.pyfunc.load_model(cls_model_uri)

    icu_days_pred = float(reg_model.predict(X)[0])
    icu_binary_pred = int(cls_model.predict(X)[0])

    result = {
        "caseid": caseid,
        "actual_icu_days": actual_icu,
        "regression": {
            "model": reg_model_uri,
            "predicted_icu_days": round(icu_days_pred, 2),
        },
        "classification": {
            "model": cls_model_uri,
            "predicted_needs_icu": bool(icu_binary_pred),
            "verdict": "ICU required" if icu_binary_pred else "No ICU needed",
        },
    }
    print(json.dumps(result, indent=2))
    return json.dumps(result)


@workflow
def icu_predictor_inference_workflow(caseid: str = "1") -> str:
    features = fetch_case_features(caseid=caseid)
    reg_uri = find_best_regression_model()
    cls_uri = find_best_classification_model()
    return run_predictions(features_pa=features, reg_model_uri=reg_uri, cls_model_uri=cls_uri)
