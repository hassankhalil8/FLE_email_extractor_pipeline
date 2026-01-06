import pandas as pd
from .tasks import process_firm

def start_ingestion(excel_path):
    print(f"Reading {excel_path}...")
    df = pd.read_excel(excel_path)
    
    # Assuming the column header is 'Website'
    urls = df['website'].dropna().unique().tolist()
    
    print(f"Queueing {len(urls)} firms...")
    for url in urls:
        process_firm.delay(url)
    
    print("🚀 Ingestion complete. Check Flower for progress.")

if __name__ == "__main__":
    start_ingestion("data/prospects.xlsx")