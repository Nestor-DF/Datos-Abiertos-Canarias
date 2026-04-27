import os
from dotenv import load_dotenv
from pathlib import Path

# Cargar variables de entorno desde .env
base_dir = Path(__file__).resolve().parent.parent
load_dotenv(base_dir / ".env")

DB_USER = os.getenv("DB_USER", "nestor")
DB_PASSWORD = os.getenv("DB_PASSWORD", "canarias")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "datos_canarias")

# URI de conexión de SQLAlchemy
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

DEBUG_MODE = os.getenv("DEBUG_MODE", "False").lower() in ("true", "1", "t")
MAX_RECORDS_DOWNLOAD = int(os.getenv("MAX_RECORDS_DOWNLOAD", "1000"))

# Fuentes Hardcoded Iniciales
SOURCES = [
    {
        "id": "gobcan",
        "name": "Gobierno de Canarias",
        "url": "https://datos.canarias.es/catalogos/general",
        "type": "Especializado"
    },
    {
        "id": "sitcan",
        "name": "SITCAN",
        "url": "https://opendata.sitcan.es",
        "type": "Especializado"
    },
    {
        "id": "istac",
        "name": "ISTAC",
        "url": "https://datos.canarias.es/catalogos/estadisticas",
        "type": "Especializado"
    },
    {
        "id": "tenerife",
        "name": "Cabildo de Tenerife",
        "url": "https://datos.tenerife.es/ckan",
        "type": "Cabildo"
    },
    {
        "id": "lapalma",
        "name": "Cabildo de La Palma",
        "url": "https://lapalmasmart-open.lapalma.es/datosabiertos/catalogo",
        "type": "Cabildo"
    },
    {
        "id": "elhierro",
        "name": "Cabildo de El Hierro",
        "url": "https://datosabiertos.elhierro.es",
        "type": "Cabildo"
    },
    {
        "id": "fuerteventura",
        "name": "Cabildo de Fuerteventura",
        "url": "https://gobiernoabierto.cabildofuer.es/datosabiertos/catalogo",
        "type": "Cabildo"
    },
    {
        "id": "lpgc",
        "name": "Ayuntamiento de Las Palmas de Gran Canaria",
        "url": "http://apidatosabiertos.laspalmasgc.es",
        "type": "Ayuntamiento"
    }, 
    {
        "id": "parcan",
        "name": "Parlamento de Canarias",
        "url": "https://datos.parcan.es/dataset",
        "type": "Especializado"
    }
]
