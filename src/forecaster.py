import pandas as pd
import numpy as np
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from config import engine, OUTPUT_DATA_PATH
import os
import warnings

# Suppress the massive walls of statsmodels and pandas warnings
warnings.filterwarnings("ignore")

def generate_forecasts():
    print("🧠 Starting Time-Series Forecasting...")
    
    # 1. Pull cleaned data
    df = pd.read_sql("SELECT * FROM consumption_reports_cleaned", engine)
    df['date_id'] = pd.to_datetime(df['date_id'])
    
    forecast_results = []

    # 2. Loop through each facility and drug
    for (f_id, c_id), group in df.groupby(['facility_id', 'commodity_id']):
        group = group.sort_values('date_id').set_index('date_id')
        
        try:
            # Exponential Smoothing
            model = ExponentialSmoothing(group['quantity_dispensed'], trend='add', seasonal=None)
            model_fit = model.fit()
            
            # Predict next week (using .iloc to fix the Pandas warning)
            prediction = model_fit.forecast(steps=1).iloc[0] 
            
            # Get latest stock (SQLAlchemy 2.0 compliant using pd.read_sql)
            query = f"SELECT stock_on_hand FROM inventory_logs WHERE facility_id={f_id} AND commodity_id={c_id} ORDER BY date_id DESC LIMIT 1"
            last_stock_df = pd.read_sql(query, engine)
            last_stock = last_stock_df.iloc[0, 0] if not last_stock_df.empty else 0
            
            # Logic: Calculate Depletion Date
            predicted_daily = prediction / 7
            days_left = int(last_stock / predicted_daily) if predicted_daily > 0 else 365
            depletion_date = pd.Timestamp.now() + pd.Timedelta(days=days_left)

            forecast_results.append({
                'facility_id': f_id,
                'commodity_id': c_id,
                'forecast_date': pd.Timestamp.now().date(),
                'predicted_daily_consumption': round(predicted_daily, 2),
                'days_of_stock_remaining': days_left,
                'predicted_depletion_date': depletion_date.date()
            })
        except Exception as e:
            print(f"⚠️ Failed for Facility {f_id}, Commodity {c_id}: {e}")
            continue

    # 3. Save to Database and Local Folder
    if forecast_results:
        forecast_df = pd.DataFrame(forecast_results)
        forecast_df.to_sql('forecasts', engine, if_exists='replace', index=False)
        
        if not os.path.exists(OUTPUT_DATA_PATH):
            os.makedirs(OUTPUT_DATA_PATH)
        forecast_df.to_csv(os.path.join(OUTPUT_DATA_PATH, 'forecast_results.csv'), index=False)
        
        print(f"✅ Forecasts complete! Generated {len(forecast_df)} predictions.")
    else:
        print("❌ No forecasts were generated.")

if __name__ == "__main__":
    generate_forecasts()