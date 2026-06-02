from abc import ABC, abstractmethod
import pandas as pd

class AbstractFilter(ABC):
    def __init__(self, name: str):
        self.name = name

    @abstractmethod    
    def apply(self, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        pass