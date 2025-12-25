from abc import ABC, abstractmethod


class APIInterface(ABC):
    def __init__(self, base_url: str):
        self.base_url = base_url
