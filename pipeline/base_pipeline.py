from abc import ABC, abstractmethod
from typing import Any

from infraestructure.database import DisnetManager


class IntegrationPipeline(ABC):
    @abstractmethod
    def __init__(self, db: DisnetManager, *args, **kwargs):
        pass

    @abstractmethod
    def run(self, *args: Any, **kwargs: Any) -> Any:
        pass


class ParallelablePipeline(ABC):
    @abstractmethod
    def __init__(self, db: DisnetManager, *args, **kwargs):
        pass

    @abstractmethod
    def fetch(self):
        pass

    @abstractmethod
    def persist(self):
        pass
