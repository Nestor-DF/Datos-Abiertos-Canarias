import jinja2
import os
import logging
from sqlalchemy.orm import Session
from src.database.connection import SessionLocal
from src.database.models import Source, SummaryMetrics

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def generate_report():
    db: Session = SessionLocal()
    
    # Get all summaries sorted by score
    summaries = db.query(SummaryMetrics, Source).join(Source)\
                  .filter(SummaryMetrics.source_id == Source.id)\
                  .order_by(SummaryMetrics.global_score.desc()).all()
    
    data = []
    for sum_metric, source in summaries:
        data.append({
            "name": source.name,
            "type": source.type,
            "v": sum_metric.volume_datasets,
            "r": sum_metric.total_records,
            "norm_v": round(sum_metric.normalized_v, 2),
            "norm_r": round(sum_metric.normalized_r, 2),
            "a": round(sum_metric.freshness_score_a, 2),
            "score": round(sum_metric.global_score, 2)
        })
        
    db.close()
    
    template_dir = os.path.join(os.path.dirname(__file__), '../../templates')
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_dir))
    template = env.get_template('report_template.html')
    
    html_output = template.render(data=data)
    
    output_path = os.path.join(os.path.dirname(__file__), '../../data/report.html')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_output)
    
    logging.info(f"Informe generado con éxito en {output_path}")

if __name__ == "__main__":
    generate_report()
