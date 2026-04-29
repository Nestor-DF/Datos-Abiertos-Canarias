from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseExtractor(ABC):
    def __init__(self, source_id: str, url: str):
        self.source_id = source_id
        self.url = url

    @abstractmethod
    def get_datasets_list(self) -> List[str]:
        """Devuelve una lista de IDs de los datasets disponibles."""
        pass

    @abstractmethod
    def get_dataset_details(self, dataset_id: str) -> Dict[str, Any]:
        """Devuelve detalles como título, actualización y lista de recursos."""
        pass
