import requests
import datetime
from typing import List, Dict, Any
from src.extractors.base import BaseExtractor

class CkanExtractor(BaseExtractor):
    def __init__(self, source_id: str, url: str):
        # Asegurar que no terminamos con barra para armar la URL del API
        super().__init__(source_id, url.rstrip('/'))

    def get_datasets_list(self) -> List[str]:
        api_url = f"{self.url}/api/3/action/package_list"
        try:
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
            data = response.json()
            if data.get("success"):
                return data.get("result", [])
        except requests.RequestException as e:
            raise Exception(f"Failed to fetch dataset list from {api_url}: {e}")
        return []

    def get_dataset_details(self, dataset_id: str) -> Dict[str, Any]:
        api_url = f"{self.url}/api/3/action/package_show"
        try:
            response = requests.get(api_url, params={"id": dataset_id}, timeout=30)
            response.raise_for_status()
            data = response.json()
            if data.get("success"):
                result = data.get("result", {})
                
                # Obtener la fecha de metadata modificada
                last_updated_str = result.get("metadata_modified")
                last_updated = None
                if last_updated_str:
                    try:
                        # Formato usual ISO 8601
                        last_updated = datetime.datetime.fromisoformat(last_updated_str.replace("Z", "+00:00"))
                        # Eliminar el timezone info para poder guardarlo en Postgres sin problemas, o usar UTC
                        last_updated = last_updated.replace(tzinfo=None)
                    except ValueError:
                        pass
                
                resources = []
                for res in result.get("resources", []):
                    resources.append({
                        "id": res.get("id"),
                        "title": res.get("name") or res.get("id"),
                        "format": res.get("format", "").upper(),
                        "url": res.get("url")
                    })
                
                return {
                    "id": result.get("id"),
                    "title": result.get("title") or result.get("name"),
                    "last_updated": last_updated,
                    "resources": resources
                }
        except requests.RequestException as e:
            raise Exception(f"Failed to fetch dataset {dataset_id} details from {api_url}: {e}")
            
        return {}
