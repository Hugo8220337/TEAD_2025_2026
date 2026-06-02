import json
import os
import sys
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from flytekit import ImageSpec, task, workflow

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from utils.config import SETTINGS

image_spec_ml = ImageSpec(
    name="icu_predictor_ml",
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


def _ensure_experiment(name: str) -> None:
    """Restores a deleted MLflow experiment so it can be reused, or creates it fresh."""
    import mlflow
    from mlflow.tracking import MlflowClient
    client = MlflowClient()
    experiment = client.get_experiment_by_name(name)
    if experiment is not None and experiment.lifecycle_stage == "deleted":
        client.restore_experiment(experiment.experiment_id)
    mlflow.set_experiment(name)

NUMERIC_FEATURES = [
    "age", "bmi", "asa",
    "preop_hb", "preop_plt", "preop_wbc", "preop_gluc", "preop_cr", "preop_alb",
]
CATEGORICAL_FEATURES = ["sex", "department", "optype", "approach", "ane_type"]
TARGET_COL = "icu_days"

# S3 env vars so MLflow can store artifacts in MinIO
os.environ["MLFLOW_S3_ENDPOINT_URL"] = f"http://{SETTINGS.s3_endpoint}"
os.environ["AWS_ACCESS_KEY_ID"] = SETTINGS.s3_access_key
os.environ["AWS_SECRET_ACCESS_KEY"] = SETTINGS.s3_secret_key
os.environ["AWS_DEFAULT_REGION"] = SETTINGS.s3_region
os.environ["AWS_EC2_METADATA_DISABLED"] = "true"


@task(container_image=image_spec_ml)
def load_feature_table() -> pa.Table:
    catalog = load_catalog("hive_prod", **SETTINGS.catalog_config_gold)
    return catalog.load_table(
        f"{SETTINGS.namespace_gold}.{SETTINGS.table_gold_icu_ml_features}"
    ).scan().to_arrow()


def _build_preprocessor(numeric_features: list, categorical_features: list):
    from sklearn.compose import ColumnTransformer
    from sklearn.impute import SimpleImputer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OrdinalEncoder, StandardScaler

    numeric_pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
    ])
    categorical_pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("encoder", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)),
    ])
    return ColumnTransformer([
        ("num", numeric_pipe, numeric_features),
        ("cat", categorical_pipe, categorical_features),
    ])


def _cv_scores(model, X, y, scoring, cv=5):
    from sklearn.model_selection import cross_validate
    results = cross_validate(model, X, y, scoring=scoring, cv=cv, return_train_score=False)
    return {k: (float(v.mean()), float(v.std())) for k, v in results.items()}


@task(container_image=image_spec_ml)
def train_regression_models(features_pa: pa.Table) -> str:
    import mlflow
    import pandas as pd
    from sklearn.linear_model import LinearRegression
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.pipeline import Pipeline
    from xgboost import XGBRegressor

    df = features_pa.to_pandas()
    df = df.dropna(subset=[TARGET_COL])

    # Drop features that are 100% NaN — they carry no information and break the imputer
    active_numeric = [f for f in NUMERIC_FEATURES if f in df.columns and df[f].notna().any()]
    active_categorical = [f for f in CATEGORICAL_FEATURES if f in df.columns]
    dropped = set(NUMERIC_FEATURES) - set(active_numeric)
    if dropped:
        print(f"Dropping all-NaN numeric features: {sorted(dropped)}")

    X = df[active_numeric + active_categorical]
    y = df[TARGET_COL].astype(float)

    preprocessor = _build_preprocessor(active_numeric, active_categorical)
    models = {
        "LinearRegression": LinearRegression(),
        "RandomForestRegressor": RandomForestRegressor(n_estimators=100, random_state=42),
        "XGBRegressor": XGBRegressor(n_estimators=100, random_state=42, verbosity=0),
    }
    scoring = {
        "neg_rmse": "neg_root_mean_squared_error",
        "neg_mae": "neg_mean_absolute_error",
        "r2": "r2",
    }

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    _ensure_experiment(EXPERIMENT_REGRESSION)

    summaries = {}
    for name, estimator in models.items():
        pipe = Pipeline([("preprocessor", preprocessor), ("model", estimator)])
        scores = _cv_scores(pipe, X, y, scoring=scoring)

        with mlflow.start_run(run_name=name):
            mlflow.set_tags({"model_type": name, "problem": "regression", "target": TARGET_COL})
            mlflow.log_params({
                "n_numeric_features": len(active_numeric),
                "n_categorical_features": len(active_categorical),
                "cv_folds": 5,
                "n_samples": len(df),
                "dropped_features": str(sorted(dropped)),
            })
            metrics = {
                "rmse_mean": -scores["test_neg_rmse"][0],
                "rmse_std": scores["test_neg_rmse"][1],
                "mae_mean": -scores["test_neg_mae"][0],
                "mae_std": scores["test_neg_mae"][1],
                "r2_mean": scores["test_r2"][0],
                "r2_std": scores["test_r2"][1],
            }
            mlflow.log_metrics(metrics)
            pipe.fit(X, y)
            mlflow.sklearn.log_model(pipe, artifact_path="model", registered_model_name=f"icu_{name}")
            summaries[name] = metrics
            print(f"[{name}] RMSE={metrics['rmse_mean']:.3f} ± {metrics['rmse_std']:.3f}  R²={metrics['r2_mean']:.3f}")

    return json.dumps(summaries)


@task(container_image=image_spec_ml)
def train_classification_models(features_pa: pa.Table) -> str:
    import mlflow
    import pandas as pd
    from sklearn.linear_model import LogisticRegression
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.pipeline import Pipeline
    from xgboost import XGBClassifier

    df = features_pa.to_pandas()
    df = df.dropna(subset=[TARGET_COL])

    # Drop features that are 100% NaN — they carry no information and break the imputer
    active_numeric = [f for f in NUMERIC_FEATURES if f in df.columns and df[f].notna().any()]
    active_categorical = [f for f in CATEGORICAL_FEATURES if f in df.columns]
    dropped = set(NUMERIC_FEATURES) - set(active_numeric)
    if dropped:
        print(f"Dropping all-NaN numeric features: {sorted(dropped)}")

    X = df[active_numeric + active_categorical]
    y = (df[TARGET_COL] > 0).astype(int)

    preprocessor = _build_preprocessor(active_numeric, active_categorical)
    models = {
        "LogisticRegression": LogisticRegression(max_iter=1000, random_state=42),
        "RandomForestClassifier": RandomForestClassifier(n_estimators=100, random_state=42),
        "XGBClassifier": XGBClassifier(n_estimators=100, random_state=42, verbosity=0, eval_metric="logloss"),
    }
    scoring = {
        "roc_auc": "roc_auc",
        "f1": "f1",
        "accuracy": "accuracy",
    }

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    _ensure_experiment(EXPERIMENT_CLASSIFICATION)

    summaries = {}
    for name, estimator in models.items():
        pipe = Pipeline([("preprocessor", preprocessor), ("model", estimator)])
        scores = _cv_scores(pipe, X, y, scoring=scoring)

        with mlflow.start_run(run_name=name):
            mlflow.set_tags({"model_type": name, "problem": "classification", "target": "icu_binary"})
            class_balance = y.value_counts().to_dict()
            mlflow.log_params({
                "n_numeric_features": len(active_numeric),
                "n_categorical_features": len(active_categorical),
                "cv_folds": 5,
                "n_samples": len(df),
                "n_positive": int(class_balance.get(1, 0)),
                "n_negative": int(class_balance.get(0, 0)),
                "dropped_features": str(sorted(dropped)),
            })
            metrics = {
                "auc_mean": scores["test_roc_auc"][0],
                "auc_std": scores["test_roc_auc"][1],
                "f1_mean": scores["test_f1"][0],
                "f1_std": scores["test_f1"][1],
                "accuracy_mean": scores["test_accuracy"][0],
                "accuracy_std": scores["test_accuracy"][1],
            }
            mlflow.log_metrics(metrics)
            pipe.fit(X, y)
            mlflow.sklearn.log_model(pipe, artifact_path="model", registered_model_name=f"icu_{name}")
            summaries[name] = metrics
            print(f"[{name}] AUC={metrics['auc_mean']:.3f} ± {metrics['auc_std']:.3f}  F1={metrics['f1_mean']:.3f}")

    return json.dumps(summaries)


@workflow
def icu_predictor_train_workflow() -> tuple[str, str]:
    features = load_feature_table()
    reg_summary = train_regression_models(features_pa=features)
    cls_summary = train_classification_models(features_pa=features)
    return reg_summary, cls_summary
