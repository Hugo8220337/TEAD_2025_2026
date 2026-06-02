import os
import vitaldb
import pandas as pd

MAX_CASES = 1000

def fetch_vitals(
    num_cases: int, 
    interval_sec: int = 1, 
    output_file: str = None
):
    if output_file is None:
        output_file = "vitals.csv"

    first_case = True
    start_case = 1

    if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
        try:
            existing = pd.read_csv(output_file, usecols=['Case_ID'])
            last_case = existing['Case_ID'].max()
            if pd.notna(last_case):
                start_case = int(last_case) + 1
                first_case = False
                print(f"Resuming from case {start_case} (last found: {int(last_case)})")
        except Exception as e:
            print(f"Could not read existing file, starting from case 1: {e}")

    if start_case > num_cases:
        print("All cases already fetched.")
        return

    print(f"Fetching cases {start_case} to {num_cases}...")

    tracks = vitaldb.get_track_names(list(range(1, MAX_CASES + 1)))
    track_list = tracks['tnames'].explode().unique().tolist()

    for case_id in range(start_case, num_cases + 1):
        try:
            df = vitaldb.vital_recs(
                [case_id], 
                track_names=track_list,
                interval=interval_sec, 
                return_pandas=True, 
                return_timestamp=True
            )
            
            if df is not None and not df.empty:
                df.insert(0, 'Case_ID', case_id)
                
                mode = 'w' if first_case else 'a'
                header = first_case
                df.to_csv(output_file, index=False, mode=mode, header=header)
                
                first_case = False
                print(f"Successfully saved case {case_id}")
            else:
                print(f"No data found for case {case_id}.")
                
        except Exception as e:
            print(f"Failed to fetch case {case_id}: {e}")
            
if __name__ == "__main__":
    fetch_vitals(num_cases=MAX_CASES, interval_sec=5)
