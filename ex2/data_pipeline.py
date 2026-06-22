#!/usr/bin/env python3
from typing import Any, Protocol
from abc import ABC, abstractmethod


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class JsonExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        result = {f"item_{i[0]}": f"{i[1]}" for i in data}
        print(result)


class CsvExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        for i in data:
            print(i[1], end="")
            if i != data[-1]:
                print(",", end="")
        print()


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[Any] = []
        self._counter: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        self._counter += 1
        return (self._counter - 1, self._data.pop(0))

    def get_counter(self) -> int:
        return self._counter


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


class DataStream():
    def __init__(self) -> None:
        self._num: NumericProcessor
        self._text: TextProcessor
        self._log: LogProcessor
        self._num_exist: bool = False
        self._text_exist: bool = False
        self._log_exist: bool = False
        self._num_count: int = 0
        self._text_count: int = 0
        self._log_count: int = 0

    def register_processor(self, proc: DataProcessor) -> None:
        if type(proc) is NumericProcessor and not self._num_exist:
            self._num = proc
            self._num_exist = True
        elif type(proc) is TextProcessor and not self._text_exist:
            self._text = proc
            self._text_exist = True
        elif type(proc) is LogProcessor and not self._log_exist:
            self._log = proc
            self._log_exist = True
        else:
            print(f"Procesor {proc} exist or wrong type procesor")

    def process_stream(self, stream: list[Any]) -> None:
        for item in stream:
            if self._num_exist and self._num.validate(item):
                self._num.ingest(item)
                if type(item) is list:
                    self._num_count += len(item)
                else:
                    self._num_count += 1
            elif self._text_exist and self._text.validate(item):
                self._text.ingest(item)
                if type(item) is list:
                    self._text_count += len(item)
                else:
                    self._text_count += 1
            elif self._log_exist and self._log.validate(item):
                self._log.ingest(item)
                if type(item) is list:
                    self._log_count += len(item)
                else:
                    self._log_count += 1
            else:
                print("DataStream error - Can't process "
                      f"element in stream: {item}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if self._num_exist:
            print(f"Numeric Processor: total {self._num_count} items proces"
                  f"sed, remaining {self._num_count - self._num.get_counter()}"
                  " on processor")
        if self._text_exist:
            print(f"Text Processor: total {self._text_count} items processed, "
                  f"remaining {self._text_count - self._text.get_counter()} "
                  "on processor")
        if self._log_exist:
            print(f"Log Processor: total {self._log_count} items processed, "
                  f"remaining {self._log_count - self._log.get_counter()} "
                  "on processor")
        if (not self._num_exist
            and not self._text_exist
                and not self._log_exist):
            print("No processor found, no data")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        output_list: list[tuple[int, str]] = []
        if self._num_exist:
            output_list.clear()
            for _ in range(nb):
                if self._num_count - self._num.get_counter():
                    output_list.append(self._num.output())
            plugin.process_output(output_list)
        if self._text_exist:
            output_list.clear()
            for _ in range(nb):
                if self._text_count - self._text.get_counter():
                    output_list.append(self._text.output())
            plugin.process_output(output_list)
        if self._log_exist:
            output_list.clear()
            for _ in range(nb):
                if self._log_count - self._log.get_counter():
                    output_list.append(self._log.output())
            plugin.process_output(output_list)


def test() -> None:
    print("=== Code Nexus - Data Pipeline ===\n")
    print("\nInitialize Data Stream...\n")
    stream = DataStream()
    stream.print_processors_stats()
    print("Registering Processors\n")
    num = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()
    stream.register_processor(num)
    stream.register_processor(text)
    stream.register_processor(log)
    first_data = ['Hello world', [3.14, -1, 2.71],
                  [{'log_level': 'WARNING', 'log_message':
                    'Telnet access! Use ssh instead'},
                   {'log_level': 'INFO', 'log_message':
                    'User wil isconnected'}], 42, ['Hi', 'five']]
    print(f"Send first batch of data on stream: {first_data}\n")
    stream.process_stream(first_data)
    stream.print_processors_stats()
    print("\nSend 3 processed data from each processor to a CSV plugin:")
    stream.output_pipeline(3, CsvExportPlugin())
    print()
    stream.print_processors_stats()
    another_data = [21, ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
                    [{'log_level': 'ERROR', 'log_message': '500 server crash'},
                     {'log_level': 'NOTICE', 'log_message':
                      'Certificate expires in 10 days'}],
                    [32, 42, 64, 84, 128, 168], 'World hello']
    print(f"\nSend another batch of data: {another_data}\n")
    stream.process_stream(another_data)
    stream.print_processors_stats()
    print("\nSend 5 processed data from each processor to a JSON plugin:")
    stream.output_pipeline(5, JsonExportPlugin())
    print()
    stream.print_processors_stats()


if __name__ == "__main__":
    test()
