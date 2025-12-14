from abc import ABC, abstractmethod


class IntegrationPipeline(ABC):
    @abstractmethod
    def run(self):
        pass
