## Contexto del proyecto

Proyecto:
Hiperautomatización - Ranking de datos abiertos de Canarias

Misión:
En las Islas Canarias existe una red de portales de datos abiertos impulsada por Gobierno autonómico, cabildos y ayuntamientos. El mantenimiento, volumen de datos y calidad varía entre portales.
El objetivo es construir una solución automatizada (RPA/APIs) que audite y compare el estado de estos portales sin intervención manual, culminando en un dashboard analítico unificado y un informe final HTML autocontenido.

## Fuentes objetivo mínimas

La solución debe estar preparada para conectarse, como mínimo, a estos portales, estas serán las fuentes de datos:

1. Gobierno de Canarias: https://datos.canarias.es/
Tipo: Portal especializado
API: https://datos.canarias.es/catalogos/general/api/action/package_list
DCAT: https://datos.canarias.es/catalogos/general/dcat.json

2. SITCAN: https://opendata.sitcan.es 
Tipo: Portal especializado
API: https://opendata.sitcan.es/api/3/action/package_list
DCAT: https://opendata.sitcan.es/dcat.json

3. ISTAC:  https://datos.canarias.es/api/estadisticas/
Tipo: Portal especializado
API: https://datos.canarias.es/catalogos/estadisticas/api/3/action/package_list
DCAT: https://datos.canarias.es/catalogos/estadisticas/dcat.json

4. Cabildo de Tenerife: https://datos.tenerife.es/es/datos/api 
Tipo: Cabildo
API: https://datos.tenerife.es/ckan/api/3/action/package_list 
DCAT: https://datos.tenerife.es/ckan/dcat.json

5. Cabildo de La Palma: https://lapalmasmart-open.lapalma.es/datosabiertos/api 
Tipo: Cabildo
API: https://lapalmasmart-open.lapalma.es/datosabiertos/catalogo/api/3/action/package_list
DCAT: https://lapalmasmart-open.lapalma.es/datosabiertos/catalogo/dcat.json

6. Cabildo de El Hierro: https://datosabiertos.elhierro.es 
Tipo: Cabildo
API: https://datosabiertos.elhierro.es/api/3/action/package_list
DCAT: https://datosabiertos.elhierro.es/dcat.json

7. Cabildo de Fuerteventura: https://gobiernoabierto.cabildofuer.es/es/datosabiertos/api 
Tipo: Cabildo
API: https://gobiernoabierto.cabildofuer.es/datosabiertos/catalogo/api/3/action/package_list
DCAT: https://gobiernoabierto.cabildofuer.es/datosabiertos/catalogo/dcat.json (No funciona, da internal server error)

8. Ayuntamiento de Las Palmas de Gran Canaria: http://datosabiertos.laspalmasgc.es/info-api/ 
Tipo: Ayuntamiento
API: http://apidatosabiertos.laspalmasgc.es/api/3/action/package_list
DCAT: http://apidatosabiertos.laspalmasgc.es/dcat.json (No funciona, no tenemos permisos)

9. Parlamento de Canarias: https://datos.parcan.es
Tipo: Portal especializado
API: https://parcan.es/api/

La arquitectura debe ser extensible para añadir más fuentes sin romper el diseño. Incluso si sus APIs no están basadas en CKAN.

## Objetivo funcional

La aplicación/sistema debe:

1. Extraer la lista de datasets disponibles en cada una de las fuentes de datos.
2. Descargar el contenido de cada dataset listado en el punto 1.
En caso de que los puntos 1 y/o 2 fallen en la descarga de los datos se debe dejar registrado en un log y pasar a la siguiente fuente de datos.
3. Identificar datasets únicos y su última versión disponible.
4. Calcular cuántos datasets existen en cada fuente.
5. Calcular cuántos registros contiene cada dataset, trabajando sobre la última versión.
6. Determinar cuántas versiones existen por dataset.
7. Analizar la fecha de última actualización de cada dataset.
8. Si un dataset no tiene fecha de actualización, asumir que está actualizado.
9. Persistir toda la información en PostgreSQL.
10. Ser totalmente configurable y ejecutable automáticamente con Python + Docker.
11. Ser resiliente: si la ejecución se interrumpe, debe poder reanudarse desde el último punto consistente.
12. Generar tablas resumen por fuente.
13. Generar un informe final HTML autocontenido, visualizable localmente en navegador.
15. Dejar el proyecto preparado para publicar el informe final en GitHub Pages.

## KPIs y métricas obligatorias

El modelo debe calcular como mínimo:

- V = Volumen
  Número total de datasets de la fuente.

- R = Registros
  Número total de registros por dataset.

- A = Actualización
  Métrica de frescura ponderada por registros.

- Despliegue
  Número total de recursos, archivos individuales o endpoints vinculados.

- Apertura y formatos
  Ratio de formatos reutilizables (CSV, JSON) y uso de licencias abiertas.

## Fórmula global obligatoria

La nota global de ranking debe seguir esta fórmula:

Nota = (0.3 * V) + (0.3 * R) + (0.4 * A)

Condiciones:
- V y R deben normalizarse sobre el máximo valor encontrado entre instituciones comparables.
- A penaliza la antigüedad.

## Cálculo detallado de actualización (A)

Programar esta lógica:

1. Nota_Frescura_Dataset = MAX(0, 100 - (Dias_Antiguedad / 730) * 100)

2. A_institucion = SUM(Nota_Frescura_Dataset * Registros_Dataset) / SUM(Registros_Totales)

Si no hay fecha de actualización, asumir dataset actualizado.

## Comparaciones homogéneas

La solución debe dejar preparado el sistema para comparar de forma homogénea:
- ayuntamientos con ayuntamientos
- cabildos con cabildos
- portales especializados con especializados

## Requisitos técnicos

- Lenguaje principal: Python
- Base de datos: PostgreSQL
- Contenedorización: Docker / Docker Compose
- Configuración por variables de entorno
- Checkpoints / estado de ejecución para reanudación
- Preparado para CI en GitHub Actions
- Informe final HTML autocontenido
- El diseño debe ser mantenible y modular

## Modelo de datos mínimo esperado
Por cada fuente de datos una tabla donde cada registro sea un dataset. Cada registro se enlazará a otra tabla que será el contenido real del dataset. 
Una tabla resumen con la nota global y métricas por fuente de datos.
Una tabla para guardar logs de ejecución.
