-- 0. Clean slate (Drop existing tables if they exist)
DROP TABLE IF EXISTS forecasts CASCADE;
DROP TABLE IF EXISTS consumption_reports CASCADE;
DROP TABLE IF EXISTS inventory_logs CASCADE;
DROP TABLE IF EXISTS commodities CASCADE;
DROP TABLE IF EXISTS facilities CASCADE;
DROP TABLE IF EXISTS date_dimension CASCADE;

-- 1. Date Dimension Table (The Time-Series Backbone)
CREATE TABLE date_dimension (
    date_id DATE PRIMARY KEY,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day_of_month INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    epi_week INT NOT NULL, -- Crucial for tracking weekly health outbreaks/consumption
    is_weekend BOOLEAN
);

-- (Run this block to instantly populate 5 years of dates into the Date Dimension!)
INSERT INTO date_dimension
SELECT 
    datum AS date_id,
    EXTRACT(YEAR FROM datum) AS year,
    EXTRACT(QUARTER FROM datum) AS quarter,
    EXTRACT(MONTH FROM datum) AS month,
    TO_CHAR(datum, 'Month') AS month_name,
    EXTRACT(DAY FROM datum) AS day_of_month,
    EXTRACT(ISODOW FROM datum) AS day_of_week,
    TO_CHAR(datum, 'Day') AS day_name,
    EXTRACT(WEEK FROM datum) AS epi_week,
    CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM (
    -- Generates daily dates from Jan 1, 2020 to Dec 31, 2025
    SELECT generate_series('2020-01-01'::DATE, '2025-12-31'::DATE, '1 day'::INTERVAL) AS datum
) AS date_sequence;


-- 2. Facilities Table
CREATE TABLE facilities (
    facility_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    county VARCHAR(100) NOT NULL,
    is_asal BOOLEAN DEFAULT FALSE, 
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8)
);

-- 3. Commodities Table
CREATE TABLE commodities (
    commodity_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL, 
    unit_of_issue VARCHAR(50) 
);

-- 4. Warehouse Inventory Logs
CREATE TABLE inventory_logs (
    log_id SERIAL PRIMARY KEY,
    facility_id INT REFERENCES facilities(facility_id),
    commodity_id INT REFERENCES commodities(commodity_id),
    date_id DATE REFERENCES date_dimension(date_id), -- Links to Time-Series Table
    stock_on_hand INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. ODK/DHIS2 Consumption Reports
CREATE TABLE consumption_reports (
    report_id SERIAL PRIMARY KEY,
    facility_id INT REFERENCES facilities(facility_id),
    commodity_id INT REFERENCES commodities(commodity_id),
    date_id DATE REFERENCES date_dimension(date_id), -- Links to Time-Series Table
    quantity_dispensed INT NOT NULL,
    stock_out_days INT DEFAULT 0, -- Tracks how many days in this period they had 0 stock
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 6. Forecasts Table (Statsmodels Output)
CREATE TABLE forecasts (
    forecast_id SERIAL PRIMARY KEY,
    facility_id INT REFERENCES facilities(facility_id),
    commodity_id INT REFERENCES commodities(commodity_id),
    forecast_date DATE REFERENCES date_dimension(date_id), -- Date prediction was run
    predicted_daily_consumption DECIMAL(10, 2),
    days_of_stock_remaining INT,
    predicted_depletion_date DATE, -- The exact date they will hit 0
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);