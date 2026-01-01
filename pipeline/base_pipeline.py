from abc import ABC, abstractmethod

from infraestructure.database import DisnetManager


class IntegrationPipeline(ABC):
    @abstractmethod
    def __init__(self, db: DisnetManager):
        pass

    @abstractmethod
    def run(self):
        pass


class ParallelablePipeline(ABC):
    @abstractmethod
    def __init__(self, db: DisnetManager):
        pass

    @abstractmethod
    def fetch(self):
        pass

    @abstractmethod
    def persist(self):
        pass
