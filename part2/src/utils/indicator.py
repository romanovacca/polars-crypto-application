from abc import ABCMeta, abstractmethod
from typing import List


class Indicator(metaclass=ABCMeta):
    def __init__(self, name: str, type: List, window: int):
        self.name = name
        self.type = type
        self.window = window

    @abstractmethod
    def run(self, *args):
        pass

    @abstractmethod
    def get_name(self):
        return self.name

    @abstractmethod
    def get_type(self):
        return self.type

    @abstractmethod
    def get_window(self):
        return self.window
