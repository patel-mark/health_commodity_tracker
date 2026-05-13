import os
import pandas as pd
from config import engine, OUTPUT_DATA_PATH

def export_to_csv():
    print("📦 Starting Export for Power BI...")
    
    # Ensure output directory exists
    if not os.path.exists(OUTPUT_DATA_PATH):
        os.makedirs(OUTPUT_DATA_PATH)

    # 1. Export the Master View (Historical & Current Status)
    try:
        print("Fetching 'v_commodity_status' view...")
        df_status = pd.read_sql("SELECT * FROM v_commodity_status", engine)
        status_path = os.path.join(OUTPUT_DATA_PATH, 'powerbi_commodity_status.csv')
        df_status.to_csv(status_path, index=False)
        print(f"✅ Exported Master View: {status_path}")
    except Exception as e:
        print(f"❌ Failed to export Master View. Did you create the SQL view in DataGrip? Error: {e}")

    # 2. Export the Forecasts Table
    try:
        print("Fetching 'forecasts' table...")
        df_forecasts = pd.read_sql("SELECT * FROM forecasts", engine)
        forecasts_path = os.path.join(OUTPUT_DATA_PATH, 'powerbi_forecasts.csv')
        df_forecasts.to_csv(forecasts_path, index=False)
        print(f"✅ Exported Forecasts: {forecasts_path}")
    except Exception as e:
        print(f"❌ Failed to export Forecasts. Error: {e}")

    print("🏁 All Power BI exports complete. You can now transfer these files!")

if __name__ == "__main__":
    export_to_csv()