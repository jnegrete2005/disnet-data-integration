from abc import ABC, abstractmethod

from infraestructure.database import DisnetManager


class IntegrationPipeline(ABC):
    @abstractmethod
    def __init__(self, db: DisnetManager):
        pass

    @abstractmethod
    def run(self):
        pass
