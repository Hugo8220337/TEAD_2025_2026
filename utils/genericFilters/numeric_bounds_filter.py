import pandas as pd

from utils.AbstractFilter import AbstractFilter

class NumericBoundsFilter(AbstractFilter):
    """
    This filter is designed to clean specified numeric columns by enforcing defined minimum and/or maximum bounds.
    Any value that falls outside of these bounds will be set to NaN, and a record of the original value and the
    reason for its removal will be stored in a quarantine log.
    """
    def __init__(self, columns: list, min_val: float = None, max_val: float = None):
        super().__init__("NumericBoundsFilter")
        self.columns = columns
        self.min_val = min_val
        self.max_val = max_val
        self._quarantine_records = pd.DataFrame()

    def apply(self, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        for col in self.columns:
            if col in data.columns:
                bad_mask = pd.Series(False, index=data.index)

                # Evaluate the minimum limit (if defined)
                if self.min_val is not None:
                    bad_mask = bad_mask | (data[col] <= self.min_val)
                
                # Evaluate the maximum limit (if defined)
                if self.max_val is not None:
                    bad_mask = bad_mask | (data[col] >= self.max_val)

                bad_data = data[bad_mask]
                
                if not bad_data.empty:
                    # Create a quarantine log for the bad records
                    q_df = pd.DataFrame({
                        'case_id': bad_data['case_id'],
                        'time': bad_data['time'],
                        'sensor_name': col,
                        'original_value': bad_data[col],
                        'error_reason': f'Out of bounds (min: {self.min_val}, max: {self.max_val})'
                    })
                    self._quarantine_records = pd.concat([self._quarantine_records, q_df], ignore_index=True)

                # Clean the original value
                data.loc[bad_mask, col] = pd.NA
                
        return data, self._quarantine_records