import pandas as pd
from config import engine, PROCESSED_DATA_PATH
import os

def run_etl_pipeline():
    print("🔍 Extracting raw data from Postgres...")
    df = pd.read_sql("SELECT * FROM consumption_reports", engine)

    print("🛠 Transforming: Applying Advanced Anomaly Detection...")
    # Calculate rolling average to find reporting gaps
    df['rolling_avg'] = df.groupby(['facility_id', 'commodity_id'])['quantity_dispensed'].transform(lambda x: x.expanding().mean())
    df['is_anomaly'] = df['quantity_dispensed'] < (df['rolling_avg'] * 0.3)
    
    # --- NEW: SAVE TO PROCESSED FOLDER ---
    if not os.path.exists(PROCESSED_DATA_PATH):
        os.makedirs(PROCESSED_DATA_PATH)
    
    file_path = os.path.join(PROCESSED_DATA_PATH, 'cleaned_consumption_data.csv')
    df.to_csv(file_path, index=False)
    print(f"📂 Processed data saved locally to: {file_path}")

    # Load to Postgres
    print("🚀 Loading cleaned data to 'consumption_reports_cleaned' table...")
    df.to_sql('consumption_reports_cleaned', engine, if_exists='replace', index=False)
    print("🏁 ETL Pipeline complete.")

if __name__ == "__main__":
    run_etl_pipeline()