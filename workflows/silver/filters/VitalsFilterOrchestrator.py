import pandas as pd

from utils.genericFilters.numeric_bounds_filter import NumericBoundsFilter
from utils.genericFilters.time_anomaly_filter import TimeAnomalyFilter

class VitalsFilterOrchestrator():
    """
    This class is responsible for applying all the filters in /filters to the data.
    It will be used in the Silver workflow to clean and transform the data before saving it in the Silver layer.
    """
    def __init__(self):
        sensors_must_be_positive = [
            'solar8000_hr',         # Frequência Cardíaca do monitor Solar8000
            'solar8000_art_sbp',    # Pressões Arteriais do monitor Solar8000
            'solar8000_art_dbp',    # Pressões Arteriais do monitor Solar8000
            'solar8000_art_mbp',    # Pressões Arteriais do monitor Solar8000
            'bis_bis',              # BIS do monitor BIS
            'bis_sqi'               # SQI do monitor BIS
        ]
        
        sensors_percentage = [
            'solar8000_pleth_spo2', # SpO2 do oxímetro de pulso
            'invos_sco2_l',         # SCO2 do INVOS (lado esquerdo)
            'invos_sco2_r'          # SCO2 do INVOS (lado direito)
        ]

        sensors_ventilator_pressure = [
            'primus_awp',           # AWP do ventilador Primus
            'primus_mawp_mbar',     # MAWP do ventilador Primus
            'solar8000_vent_mawp'   # MAWP do ventilador Solar8000
        ]
        
        self.filters = [
            TimeAnomalyFilter(filters_to_apply=["null"], time_col='time'),
            
            # Pressões e Frequências: Têm de ser estritamente maiores que 0
            NumericBoundsFilter(columns=sensors_must_be_positive, min_val=0),
            
            # Percentagens de Oxigénio: Têm de estar entre 0 e 100
            NumericBoundsFilter(columns=sensors_percentage, min_val=-0.001, max_val=100.001), 
            
            # Pressões de Ventilador: Toleram vácuo ligeiro (-5), mas não valores extremos
            NumericBoundsFilter(columns=sensors_ventilator_pressure, min_val=-5.0)
        ]

    def apply_filters(self, data: pd.DataFrame) -> pd.DataFrame:
        all_quarantine = []
        
        for curr_filter in self.filters:
            # Clean data with the current filter
            data, quarantine = curr_filter.apply(data)
            
            if not quarantine.empty:
                all_quarantine.append(quarantine)
                
        # Fuse all quarantine records into a single DataFrame (if there are any)
        quarantine_df = pd.concat(all_quarantine, ignore_index=True) if all_quarantine else pd.DataFrame()
        
        return data, quarantine_df 