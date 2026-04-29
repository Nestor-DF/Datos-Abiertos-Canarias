import jinja2
import os
import json
import re
import logging
from sqlalchemy.orm import Session
from sqlalchemy import func
from src.database.connection import SessionLocal
from src.database.models import Source, SummaryMetrics
import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def _load_previous_report_data(snapshot_path: str) -> list[dict]:
    """Carga los datos usados en la última generación del informe, si existen."""
    if not os.path.exists(snapshot_path):
        return []

    try:
        with open(snapshot_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except (json.JSONDecodeError, OSError) as exc:
        logging.warning(f"No se pudo leer el snapshot previo del informe: {exc}")
        return []


def _load_previous_report_data_from_html(report_path: str) -> list[dict]:
    """Intenta recuperar datos del informe HTML previo si aún no existe snapshot JSON.

    Es un mecanismo de compatibilidad para no perder fuentes ya presentes en
    informes generados antes de introducir `report_data.json`.
    """
    if not os.path.exists(report_path):
        return []

    try:
        with open(report_path, "r", encoding="utf-8") as f:
            html = f.read()
    except OSError as exc:
        logging.warning(f"No se pudo leer el informe HTML previo: {exc}")
        return []

    row_pattern = re.compile(r"<tr>(.*?)</tr>", re.DOTALL)
    cell_pattern = re.compile(r"<td.*?>(.*?)</td>", re.DOTALL)
    tag_pattern = re.compile(r"<.*?>", re.DOTALL)
    number_pattern = re.compile(r"-?\d+(?:\.\d+)?")

    recovered_data = []

    for row_match in row_pattern.finditer(html):
        row_html = row_match.group(1)
        cells = cell_pattern.findall(row_html)
        if len(cells) < 9:
            continue

        clean_cells = [
            tag_pattern.sub(" ", cell).replace("\n", " ").strip()
            for cell in cells
        ]
        clean_cells = [re.sub(r"\s+", " ", cell) for cell in clean_cells]

        name = clean_cells[0]
        institution_type = clean_cells[1]

        numeric_values = []
        for cell in clean_cells[2:]:
            match = number_pattern.search(cell)
            numeric_values.append(match.group(0) if match else None)

        try:
            recovered_data.append({
                "source_id": name,
                "name": name,
                "type": institution_type,
                "v": int(float(numeric_values[0])) if numeric_values[0] else 0,
                "total_resources": int(float(numeric_values[1])) if numeric_values[1] else 0,
                "reusable_formats": float(numeric_values[2]) if numeric_values[2] else 0,
                "r": int(float(numeric_values[3])) if numeric_values[3] else 0,
                "last_ingestion": clean_cells[6],
                "a": float(numeric_values[5]) if numeric_values[5] else 0,
                "score": float(numeric_values[6]) if numeric_values[6] else 0,
                "norm_v": 0,
                "norm_r": 0,
            })
        except (TypeError, ValueError, IndexError) as exc:
            logging.warning(f"No se pudo recuperar una fila del informe HTML previo: {exc}")

    if recovered_data:
        logging.info(
            f"Se recuperaron {len(recovered_data)} fuentes desde el informe HTML previo."
        )

    return recovered_data


def _merge_report_data(previous_data: list[dict], current_data: list[dict]) -> list[dict]:
    """Actualiza las fuentes recalculadas y conserva las fuentes previas no tratadas."""
    merged_by_source = {
        item.get("source_id", item.get("name")): item
        for item in previous_data
        if item.get("source_id") or item.get("name")
    }

    for item in current_data:
        source_key = item.get("source_id", item.get("name"))
        if source_key:
            merged_by_source[source_key] = item

    return sorted(
        merged_by_source.values(),
        key=lambda item: item.get("score", 0),
        reverse=True,
    )

def generate_report():
    db: Session = SessionLocal()
    
    # Get all summaries sorted by score
    summaries = db.query(SummaryMetrics, Source).join(Source)\
                  .filter(SummaryMetrics.source_id == Source.id)\
                  .order_by(SummaryMetrics.global_score.desc()).all()
    
    data = []
    for sum_metric, source in summaries:
        last_ingest_str = sum_metric.last_ingestion.strftime("%Y-%m-%d %H:%M") if sum_metric.last_ingestion else "N/A"

        data.append({
            "source_id": source.id,
            "name": source.name,
            "type": source.type,
            "last_ingestion": last_ingest_str,
            "v": sum_metric.volume_datasets,
            "r": sum_metric.total_records,
            "total_resources": sum_metric.total_resources,
            "reusable_formats": round(sum_metric.reusable_formats, 2),
            "norm_v": round(sum_metric.normalized_v, 2),
            "norm_r": round(sum_metric.normalized_r, 2),
            "a": round(sum_metric.freshness_score_a, 2),
            "score": round(sum_metric.global_score, 2)
        })
        
    db.close()
    
    template_dir = os.path.join(os.path.dirname(__file__), '../../templates')
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_dir))
    template = env.get_template('report_template.html')

    data_dir = os.path.join(os.path.dirname(__file__), '../../data')
    output_path = os.path.join(data_dir, 'report.html')
    snapshot_path = os.path.join(data_dir, 'report_data.json')
    os.makedirs(data_dir, exist_ok=True)

    previous_data = _load_previous_report_data(snapshot_path)
    if not previous_data:
        previous_data = _load_previous_report_data_from_html(output_path)

    merged_data = _merge_report_data(previous_data, data)

    html_output = template.render(data=merged_data)
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_output)

    with open(snapshot_path, "w", encoding="utf-8") as f:
        json.dump(merged_data, f, ensure_ascii=False, indent=2)
    
    logging.info(f"Informe generado con éxito en {output_path}")
    logging.info(f"Snapshot de datos del informe actualizado en {snapshot_path}")

if __name__ == "__main__":
    generate_report()
