# Reiniciar
sudo docker compose down -v && sudo docker compose up -d db
rm -f execution_state.json
source venv/bin/activate && PYTHONPATH=. python src/main.py

# Reporte
source venv/bin/activate && PYTHONPATH=. python src/pipeline/phase_03_metrics.py
source venv/bin/activate && PYTHONPATH=. python src/pipeline/phase_04_visualization.py



# CKAN – Guía básica de uso

## 1. Qué es CKAN

CKAN es una plataforma para **publicar, gestionar y consumir datos abiertos**.

Permite:

* gestionar datasets (conjuntos de datos)
* organizar datos en organizaciones y categorías
* acceder a los datos mediante API

---

## 2. Cómo funciona la API

### Endpoint base

```bash
https://<tu-ckan>/api/3/action/<funcion>
```

* Protocolo: HTTP (GET o POST)
* Formato: JSON

---

## 3. Ejemplo real (Canarias)

### Petición

```bash
https://datos.canarias.es/catalogos/general/api/action/package_list
```

### Respuesta

```json
{
  "success": true,
  "result": [
    "acceso-a-internet-de-las-viviendas-principales...",
    "accidentes-de-circulacion-con-victimas...",
    "accidentes-de-trabajo-con-baja..."
  ]
}
```

### Interpretación

* `success`: indica si la petición fue correcta
* `result`: lista de IDs de datasets

---

### Obtener detalle de un dataset

```bash
https://datos.canarias.es/catalogos/general/api/3/action/package_show?id=<dataset_id>
```

Devuelve:

* título
* descripción
* recursos (archivos)
* metadatos

---

## 4. Endpoints principales de CKAN

### Conjuntos de datos

```bash
GET /action/package_list
```

Devuelve todos los datasets

```bash
GET /action/package_show?id=<id>
```

Devuelve un dataset completo

```bash
GET /action/package_search?q=<query>
```

Busca datasets

```bash
GET /action/group_package_show?id=<grupo>
```

Datasets por categoría

```bash
GET /action/recently_changed_packages_activity_list
```

Actividad reciente

---

### Recursos

```bash
GET /action/resource_show?id=<id>
```

Metadatos de un recurso

```bash
GET /datastore/dump/{id}
```

Descargar recurso tabular

---

### Categorías (grupos)

```bash
GET /action/group_list
```

Lista de categorías

---

### Organizaciones

```bash
GET /action/organization_list
```

Lista de organizaciones

```bash
GET /action/organization_show?id=<id>
```

Detalle de organización

---

### Etiquetas

```bash
GET /action/tag_list
```

Lista de etiquetas

```bash
GET /action/tag_show?id=<id>
```

Detalle de etiqueta

---

## 5. Uso con Python

```python
from ckanapi import RemoteCKAN

ckan = RemoteCKAN('https://datos.canarias.es/catalogos/general')

datasets = ckan.action.package_list()
print(datasets)
```

---

## 6. Autenticación

Solo necesaria para operaciones de escritura.

Header:

```bash
Authorization: <API_TOKEN>
```

---

## 7. Conceptos clave

| Concepto          | Descripción                |
| ----------------- | -------------------------- |
| Dataset (package) | Conjunto de datos          |
| Resource          | Archivo dentro del dataset |
| Organization      | Publicador                 |
| Group             | Categoría                  |
| Tag               | Etiqueta                   |

---

# DCAT – Estándar de catálogos de datos

## 1. Qué es DCAT

DCAT (Data Catalog Vocabulary) es un estándar para:

* describir datasets
* compartir catálogos entre plataformas

Se usa en:

* datos.gob.es
* data.europa.eu

---

## 2. Relación con CKAN

CKAN:

* usa su propia API (Action API)
* puede exportar datos en formato DCAT

Equivalencias:

| CKAN     | DCAT              |
| -------- | ----------------- |
| package  | dcat:Dataset      |
| resource | dcat:Distribution |
| portal   | dcat:Catalog      |

---

## 3. Ejemplo real de DCAT (Canarias)

```json
{
  "title": "Entidades públicas del Gobierno de Canarias",
  "description": "Listado de las entidades públicas del Gobierno de Canarias.",
  "keyword": [
    "entidades",
    "entidades públicas",
    "organigrama"
  ],
  "language": ["es"],
  "landingPage": "Directorio de Unidades Administrativas y Oficinas de Registro y Atención a la Ciudadanía (DIRCAC)",
  "distribution": [
    {
      "title": "Entidades públicas del Gobierno de Canarias. (CSV)",
      "description": "Listado de las entidades públicas del Gobierno de Canarias, en formato CSV.",
      "format": "CSV",
      "byteSize": 6876,
      "accessURL": "https://datos.canarias.es/catalogos/general/dataset/c24a205e-29f8-40c0-96ea-24883b4acac4/resource/e9513559-1169-4fcb-a655-dce836cbeeb7/download/entidades_publicas.csv"
    },
    {
      "title": "Diccionario de datos",
      "description": "Diccionario de datos con estructura y campos.",
      "format": "CSV",
      "byteSize": 970,
      "accessURL": "https://datos.canarias.es/catalogos/general/dataset/c24a205e-29f8-40c0-96ea-24883b4acac4/resource/0c6bafd6-6e68-4530-893a-e0ee18b73509/download/diccionario_datos_de_las_entidades_publicas_del_gobierno_de_canarias.csv"
    }
  ]
}
```

---

## 4. Interpretación del DCAT

* `title` → nombre del dataset
* `description` → descripción
* `keyword` → etiquetas
* `distribution` → recursos disponibles

  * `accessURL` → enlace de descarga
  * `format` → tipo de archivo

---

## 5. Resumen final

* CKAN API:

  * orientada a aplicaciones
  * basada en endpoints `/action/...`
* DCAT:

  * estándar interoperable
  * describe datasets de forma estructurada

Uso típico:

1. Usar API CKAN para consumir datos
2. Usar DCAT para interoperabilidad entre portales