from abc import ABCMeta, abstractmethod
from typing import List


class Indicator(metaclass=ABCMeta):
    def __init__(self, name: str, type: List, window: int):
        """Base class for all the indicators

        :param name: Name of the indicator
        :param type: The type of indicator. e.g. volatility-based, or momentum
        :param window: The amount of datapoints that should be taken into consideration.
        """
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
