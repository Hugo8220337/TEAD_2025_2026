from labs import fetch_labs
from patients import fetch_patient_history
from vitals import fetch_vitals

def main():
    num_cases = 6388
    # fetch_labs(num_cases=num_cases)
    # fetch_patient_history(num_cases=num_cases)
    fetch_vitals(num_cases=num_cases, interval_sec=0.5)


if __name__ == "__main__":
    main()