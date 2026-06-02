import vitaldb
import pandas as pd

MAX_CASES = 6388

def fetch_patient_history(
    num_cases: int, 
    output_file: str = None, 
):
    if num_cases > MAX_CASES:
        num_cases = MAX_CASES

    if output_file is None:
        output_file = f"patient_history.csv"

    print(f"Fetching clinical data for {num_cases} cases...")
    
    caseids = list(range(1, num_cases + 1))
    
    try:
        df = vitaldb.load_clinical_data(caseids)
        
        if df is not None and not df.empty:
            df.to_csv(output_file, index=False)
            print(f"Success! Patient history dataset saved to {output_file}")
            return df
        else:
            print("No clinical data found for the requested cases.")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"Failed to fetch clinical data: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    fetch_patient_history(num_cases=MAX_CASES)