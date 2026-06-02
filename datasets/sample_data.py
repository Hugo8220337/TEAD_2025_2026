import pandas as pd
import os

def create_random_samples(csv_files, sample_size=100, random_state=42):
    """
    Reads multiple CSV files and creates a random sample for each.
    
    Args:
        csv_files (list): List of CSV filenames to process.
        sample_size (int): Number of rows to sample from each file.
        random_state (int): Seed for reproducibility.
    """
    for file_name in csv_files:
        if not os.path.exists(file_name):
            print(f"Warning: {file_name} not found.")
            continue
            
        try:
            print(f"Processing {file_name}...")
            # Load the dataset
            df = pd.read_csv(file_name)
            
            # Ensure we don't try to sample more rows than the file contains
            actual_n = min(len(df), sample_size)
            df_sample = df.sample(n=actual_n, random_state=random_state)
            
            # Define output path and save
            output_name = f"sample_{file_name}"
            df_sample.to_csv(output_name, index=False)
            print(f"Successfully saved {actual_n} rows to {output_name}")
            
        except Exception as e:
            print(f"Error sampling {file_name}: {e}")

if __name__ == "__main__":
    target_csvs = ["labs.csv", "vitals.csv", "patient_history.csv"]
    create_random_samples(target_csvs)