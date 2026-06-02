import os
import argparse

RUN_BASE_COMMAND = "pyflyte run"
WORKFLOWS_DIR = "workflows"


dict_workflows = {
    "bronze": [
        {
            "file": "workflow_vitals_bronze.py",
            "entrypoint": "workflow_vitals_bronze_layer",
            "csv_path": "vitals.csv",
        },
        {
            "file": "workflow_labs_bronze.py",
            "entrypoint": "workflow_labs_bronze_layer",
            "csv_path": "labs.csv",
        },
        {
            "file": "workflow_patients_bronze.py",
            "entrypoint": "workflow_patients_bronze_layer",
            "csv_path": "patient_history.csv",
        }
    ],
    "silver": [
        {
            "file": "workflow_labs_results.py",
            "entrypoint": "workflow_labs_results",
        },
        {
            "file": "workflow_labs_quarantine.py",
            "entrypoint": "workflow_labs_quarantine",
        },
        {
            "file": "workflow_labs_metadata.py",
            "entrypoint": "workflow_labs_metadata",
        },
        {
            "file": "workflow_patients_demographics.py",
            "entrypoint": "workflow_patients_demographics",
        },
        {
            "file": "workflow_patients_quarantine.py",
            "entrypoint": "workflow_patients_quarantine",
        },
        {
            "file": "workflow_patients_surgery.py",
            "entrypoint": "workflow_patients_surgery",
        },
        {
            "file": "workflow_sensors_metadata.py",
            "entrypoint": "workflow_sensors_metadata",
        },
        {
            "file": "workflow_vitals_clean_and_quarantine.py",
            "entrypoint": "workflow_vitals_clean_and_quarantine",
        }
    ],
    "gold": [
        {
            "file": "workflow_cross_sensor_drift.py",
            "entrypoint": "workflow_cross_sensor_drift",
        },
        {
            "file": "workflow_hardware_failures_summary.py",
            "entrypoint": "workflow_hardware_failures_summary",
        },
        {
            "file": "workflow_pre_vs_pos_operation.py",
            "entrypoint": "workflow_pre_vs_pos_gold_layer",
        },
        {
            "file": "workflow_sensor_health_summary.py",
            "entrypoint": "workflow_sensor_health_summary",
        },
        {
            "file": "workflow_sensor_view_gold.py",
            "entrypoint": "workflow_vitals_gold_layer",
        },
        {
            "file": "workflow_icu_features_gold.py",
            "entrypoint": "icu_features_gold_workflow",
        },
        {
            "file": "workflow_icu_predictor_train.py",
            "entrypoint": "icu_predictor_train_workflow",
        },
    ]
}


def run_workflow(layer: str, file: str, entrypoint: str, csv_path: str):
    file_path = os.path.join(WORKFLOWS_DIR, layer, file)

    try:
        if csv_path:
            cmd = f"{RUN_BASE_COMMAND} {file_path} {entrypoint} --csv_path {csv_path}"
        else:
            cmd = f"{RUN_BASE_COMMAND} {file_path   } {entrypoint}"
        
        print(f"Running command: {cmd}")
        os.system(cmd)
    except Exception as e:
        print(f"Error running workflow {entrypoint} from path {file_path}: {e}")

def _is_included(entrypoint: str, layer: str, execute_only: list):
    if not execute_only:
        return True
    
    if layer in execute_only:
        return True
    
    if entrypoint in execute_only:
        return True
    
    return False

def main(execute_only=None):
    for layer, workflows in dict_workflows.items():
        for wf in workflows:
            file = wf["file"]
            entrypoint = wf["entrypoint"]

            if "csv_path" in wf:
                csv_path = wf["csv_path"]
            else:
                csv_path = None

            if _is_included(entrypoint, layer, execute_only):
                run_workflow(layer, file, entrypoint, csv_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute-only", nargs="*", help="Specify which workflows or layers to run (e.g., --execute-only bronze workflow_labs_silver_layer)")
    args = parser.parse_args()
    
    main(args.execute_only)

# e.g., --execute-only bronze workflow_labs_silver_layer
# This will run everything in the bronze layer and also the workflow_labs_silver_layer,
# but skip other silver and gold workflows