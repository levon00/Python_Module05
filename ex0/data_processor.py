#!/usr/bin/env python3
from typing import Any
from abc import ABC , abstractmethod


class DataProcessor(ABC):
    def __init__(self):
        self.data = None

    @abstractmethod
    def validate(self, data: Any) -> bool:
        #check whether the input data are appropriate for the current data processor
        pass
    
    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        print("")

class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if not isinstance(data, (list, int, float)):
            return False
        if isinstance(data, int, float):
            return True
        return all(isinstance(x, (int, float)) for x in data)
    
    def ingest(self, data: Any) -> None:
        try:
            if not self.validate(data):
                raise ValueError(f"{type(data)} '{data}'")
        except ValueError as e:
            print(f"Test invalid ingestion of {e} without prior validation:")
        


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if not isinstance(data, (list, str)):
            return False
        if isinstance(data, str):
            return True
        return all(isinstance(x, (str)) for x in data)

class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if not isinstance(data, (dict, list)):
            return False
        if isinstance(data, dict):
            return all(isinstance(x, str) and isinstance(y, str) for x, y in data)
        return all(isinstance(x, str) and isinstance(y, str)
                   for z in data
                   for x,y in z)





if __name__ == "__main__":
    pass