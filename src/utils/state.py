import json
import os
from pathlib import Path

class CheckpointManager:
    def __init__(self, filepath="execution_state.json"):
        self.filepath = Path(filepath)
        self.state = self._load()

    def _load(self):
        if self.filepath.exists():
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                return {"completed_sources": [], "current_source": None, "processed_datasets": []}
        return {"completed_sources": [], "current_source": None, "processed_datasets": []}

    def save(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump(self.state, f, indent=4)

    def mark_dataset_processed(self, dataset_id):
        if dataset_id not in self.state["processed_datasets"]:
            self.state["processed_datasets"].append(dataset_id)
            self.save()

    def is_dataset_processed(self, dataset_id):
        return dataset_id in self.state["processed_datasets"]

    def mark_dataset_content_checked(self, dataset_id):
        """Marca que el contenido de este dataset ya fue comprobado en esta ejecución."""
        checked = self.state.setdefault("content_checked_datasets", [])
        if dataset_id not in checked:
            checked.append(dataset_id)
            self.save()

    def is_dataset_content_checked(self, dataset_id):
        """Devuelve True si el contenido ya fue comprobado en esta ejecución."""
        return dataset_id in self.state.get("content_checked_datasets", [])

    def reset_content_checked(self):
        """Limpia la lista de datasets con contenido comprobado (llamar al inicio de cada ejecución)."""
        self.state["content_checked_datasets"] = []
        self.save()

    def set_current_source(self, source_id):
        if self.state["current_source"] != source_id:
            self.state["current_source"] = source_id
            self.state["processed_datasets"] = []
            self.save()
            
    def mark_source_completed(self, source_id):
        if source_id not in self.state["completed_sources"]:
            self.state["completed_sources"].append(source_id)
            self.state["current_source"] = None
            self.save()
            
    def is_source_completed(self, source_id):
        return source_id in self.state["completed_sources"]