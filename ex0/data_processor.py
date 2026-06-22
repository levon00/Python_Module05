#!/usr/bin/env python3
from typing import Any
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[Any] = []
        self._counter = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        self._counter += 1
        return (self._counter - 1, self._data.pop(0))


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if not isinstance(data, (list, int, float)):
            return False
        if isinstance(data, (int, float)):
            return True
        return all(isinstance(x, (int, float)) for x in data)

    def ingest(self, data: Any) -> None:
        try:
            if not self.validate(data):
                raise ValueError("Improper numeric data")
            if isinstance(data, (int, float)):
                self._data.extend([str(data)])
            else:
                self._data.extend([str(i) for i in data])
        except ValueError as e:
            print(f"Got exception: {e}")


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if not isinstance(data, (list, str)):
            return False
        if isinstance(data, str):
            return True
        return all(isinstance(x, str) for x in data)

    def ingest(self, data: Any) -> None:
        try:
            if not self.validate(data):
                raise ValueError("Improper Text data")
            if isinstance(data, str):
                self._data.extend([data])
            else:
                self._data.extend(data)
        except ValueError as e:
            print(f"Got exception: {e}")


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if not isinstance(data, (dict, list)):
            return False
        if isinstance(data, dict):
            return (all(isinstance(x, str) and isinstance(y, str)
                    for x, y in data.items())
                    and len(data) == 2
                    and 'log_level' in data
                    and 'log_message' in data)
        return all(isinstance(x, str) and isinstance(y, str)
                   and len(z) == 2
                   and 'log_level' in z and 'log_message' in z
                   for z in data
                   for x, y in z.items())

    def ingest(self, data: Any) -> None:
        try:
            if not self.validate(data):
                raise ValueError("Improper Log data")
            if isinstance(data, dict):
                self._data.extend([f"{data['log_level']}:"
                                   f" {data['log_message']}"])
            else:
                self._data.extend([f"{z['log_level']}: {z['log_message']}"
                                   for z in data])
        except ValueError as e:
            print(f"Got exception: {e}")


def test() -> None:
    print("=== Code Nexus - Data Processor ===\n")
    print("Testing Numeric Processor...")
    num = NumericProcessor()
    answer = num.validate(42)
    print(f" Trying to validate input '42': {answer}")
    answer = num.validate('Hello')
    print(f" Trying to validate input 'Hello': {answer}")
    print(" Test invalid ingestion of string 'foo' without prior validation:")
    print(" ", end="")
    num.ingest('foo')
    print(" Processing data: [1, 2, 3, 4, 5]")
    num.ingest([1, 2, 3, 4, 5])
    print(" Extracting 3 values...")
    for _ in range(3):
        save = num.output()
        print(f" Numeric value {save[0]}: {save[1]}")
    print("\nTesting Text Processor...")
    text = TextProcessor()
    answer = text.validate(42)
    print(f" Trying to validate input '42': {answer}")
    print(" Processing data: ['Hello', 'Nexus', 'World']")
    text.ingest(['Hello', 'Nexus', 'World'])
    print(" Extracting 1 value...")
    save = text.output()
    print(f" Text value {save[0]}: {save[1]}")
    print("\nTesting Log Processor...")
    log = LogProcessor()
    answer = log.validate('Hello')
    print(f" Trying to validate input 'Hello': {answer}")
    print(" Processing data: [{'log_level': 'NOTICE', 'log_message': "
          "'Connection to server'}, {'log_level': 'ERROR', 'log_message': "
          "'Unauthorized access!!'}]")
    log.ingest([{'log_level': 'NOTICE', 'log_message':
                 'Connection to server'}, {'log_level': 'ERROR', 'log_message':
                                           'Unauthorized access!!'}])
    print(" Extracting 2 values...")
    for _ in range(2):
        save = log.output()
        print(f" Log entry {save[0]}: {save[1]}")


if __name__ == "__main__":
    test()
