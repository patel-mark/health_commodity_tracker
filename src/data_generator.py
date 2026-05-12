import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime, timedelta

# 1. Load Environment Variables
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Create Database Connection
connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

def seed_static_tables():
    """Populates Facilities and Commodities."""
    facilities_data = [
        {'name': 'Lodwar Referral Hospital', 'county': 'Turkana', 'is_asal': True, 'latitude': 3.11, 'longitude': 35.60},
        {'name': 'Marsabit County Hospital', 'county': 'Marsabit', 'is_asal': True, 'latitude': 2.33, 'longitude': 37.99},
        {'name': 'Garissa Provincial Hospital', 'county': 'Garissa', 'is_asal': True, 'latitude': -0.45, 'longitude': 39.66},
        {'name': 'Kiambu Level 5 Hospital', 'county': 'Kiambu', 'is_asal': False, 'latitude': -1.17, 'longitude': 36.83},
        {'name': 'Kilifi County Hospital', 'county': 'Kilifi', 'is_asal': False, 'latitude': -3.63, 'longitude': 39.85},
    ]
    df_facilities = pd.DataFrame(facilities_data)
    
    commodities_data = [
        {'name': 'AL (Artemether-Lumefantrine)', 'category': 'Malaria', 'unit_of_issue': 'Tablet'},
        {'name': 'Oxytocin Injectable', 'category': 'Maternal Health', 'unit_of_issue': 'Vial'},
        {'name': 'Rapid Diagnostic Test (RDT)', 'category': 'Malaria', 'unit_of_issue': 'Kit'},
        {'name': 'Magnesium Sulfate', 'category': 'Maternal Health', 'unit_of_issue': 'Ampoule'}
    ]
    df_commodities = pd.DataFrame(commodities_data)

    # Use engine.begin() to ensure transactions are committed
    with engine.begin() as conn:
        df_facilities.to_sql('facilities', conn, if_exists='append', index=False)
        df_commodities.to_sql('commodities', conn, if_exists='append', index=False)
    
    print("✅ Static tables seeded.")

def generate_time_series_data():
    """Generates consumption and inventory logs from 2020 to 2025."""
    
    # Updated SQLAlchemy 2.0 syntax using text() and connections
    with engine.connect() as conn:
        facility_ids = [row[0] for row in conn.execute(text("SELECT facility_id FROM facilities")).fetchall()]
        commodity_ids = [row[0] for row in conn.execute(text("SELECT commodity_id FROM commodities")).fetchall()]
    
    dates = pd.date_range(start="2020-01-01", end="2025-12-31", freq='W')
    
    consumption_list = []
    inventory_list = []

    for f_id in facility_ids:
        for c_id in commodity_ids:
            current_stock = np.random.randint(500, 1000)
            for d in dates:
                month = d.month
                base_demand = np.random.randint(10, 30)
                if month in [4, 5, 6, 10, 11]:
                    base_demand *= 2 
                
                dispensed = base_demand + np.random.randint(-5, 5)
                
                consumption_list.append({
                    'facility_id': f_id,
                    'commodity_id': c_id,
                    'date_id': d.date(),
                    'quantity_dispensed': dispensed,
                    'stock_out_days': 0 if current_stock > dispensed else np.random.randint(1, 4)
                })

                current_stock -= dispensed
                if current_stock < 100:
                    replenishment = np.random.randint(400, 600)
                    current_stock += replenishment
                
                inventory_list.append({
                    'facility_id': f_id,
                    'commodity_id': c_id,
                    'date_id': d.date(),
                    'stock_on_hand': max(0, current_stock)
                })

    with engine.begin() as conn:
        pd.DataFrame(consumption_list).to_sql('consumption_reports', conn, if_exists='append', index=False)
        pd.DataFrame(inventory_list).to_sql('inventory_logs', conn, if_exists='append', index=False)
    
    print(f"✅ Generated {len(consumption_list)} consumption records and {len(inventory_list)} inventory logs.")

if __name__ == "__main__":
    print("🚀 Starting Data Simulation...")
    seed_static_tables()
    generate_time_series_data()
    print("🏁 Database is now populated!")