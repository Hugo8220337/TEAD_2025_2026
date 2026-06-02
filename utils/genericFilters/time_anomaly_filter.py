import time
import pandas as pd

from utils.AbstractFilter import AbstractFilter

class TimeAnomalyFilter(AbstractFilter):
    """
    This filter validates the temporal structure of the data.
    It flags records where the 'time' column is null or represents a date in the future.
    """
    def __init__(self, filters_to_apply: list = None, time_col: str = 'time', max_time_epoch: float = None):
        super().__init__("TimeAnomalyFilter")
        self.time_col = time_col
        self.filters_to_apply = filters_to_apply or ["null", "future"]
        
        # Se um max_time_epoch não for fornecido, usa-se o tempo atual da máquina (em segundos Unix Epoch)
        self.max_time_epoch = max_time_epoch
        self._quarantine_records = pd.DataFrame()


    def _compute_null_filter(self, data):
        if "null" in self.filters_to_apply:
            null_mask = data[self.time_col].isna()
        else:
            null_mask = pd.Series([False] * len(data))
        return null_mask

    def _compute_future_timestamps_filter(self, data, current_limit):
        if "future" in self.filters_to_apply:
            future_mask = data[self.time_col] > current_limit
        else:
            future_mask = pd.Series([False] * len(data))
        return future_mask

    def apply(self, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        if self.time_col in data.columns:
            # Acceptable time limit
            current_limit = self.max_time_epoch if self.max_time_epoch is not None else time.time()
            
            # Condition 1: Timestamp is null
            null_mask = self._compute_null_filter(data)
            
            # Condition 2: Timestamp is in the future
            future_mask = self._compute_future_timestamps_filter(data, current_limit)
            
            # Máscara combinada (qualquer uma das violações)
            bad_mask = null_mask | future_mask
            bad_data = data[bad_mask]
            
            if not bad_data.empty:
                # Classify the reason for the error for quarantine
                error_reasons = bad_data[self.time_col].apply(
                    lambda x: 'Time is Null' if pd.isna(x) else f'Future Date (> {current_limit})'
                )

                # Create Quarentine Log
                q_df = pd.DataFrame({
                    'case_id': bad_data['case_id'] if 'case_id' in bad_data.columns else pd.NA,
                    'time': bad_data[self.time_col],
                    'sensor_name': self.time_col,
                    'original_value': bad_data[self.time_col],
                    'error_reason': error_reasons
                })
                self._quarantine_records = pd.concat([self._quarantine_records, q_df], ignore_index=True)

            # Invalida o tempo na tabela original
            data.loc[bad_mask, self.time_col] = pd.NA
            
        return data, self._quarantine_records

   

    