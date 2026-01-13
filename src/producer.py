import pandas as pd
from .tasks import process_firm

def start_ingestion(excel_path):
    print(f"Reading {excel_path}...")
    df = pd.read_excel(excel_path)

    df = df.fillna('')
    
    leads = df.to_dict(orient='records')
    
    print(f"Queueing {len(leads)} tasks...")
    for lead in leads:
        process_firm.delay(lead)
    
    print("🚀 All rows sent to Celery.")
    
    print("🚀 Ingestion complete. Check Flower for progress.")

if __name__ == "__main__":
    start_ingestion("data/exclusive_family_law_leads.xlsx")