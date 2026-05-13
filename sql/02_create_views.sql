-- sql/02_create_views.sql

-- This view merges facilities, products, and cleaned consumption 
-- for the final early-warning dashboard.
CREATE OR REPLACE VIEW v_commodity_status AS
SELECT 
    f.name AS facility_name,
    f.county,
    f.is_asal,
    c.name AS commodity_name,
    c.category,
    cr.date_id,
    cr.quantity_dispensed,
    cr.is_anomaly,
    il.stock_on_hand,
    -- Calculate Days of Stock (DOS) on hand based on current inventory
    CASE 
        WHEN cr.quantity_dispensed > 0 THEN (il.stock_on_hand / cr.quantity_dispensed) * 7
        ELSE 0 
    END AS current_dos_weekly
FROM consumption_reports_cleaned cr
JOIN facilities f ON cr.facility_id = f.facility_id
JOIN commodities c ON cr.commodity_id = c.commodity_id
JOIN inventory_logs il ON cr.facility_id = il.facility_id 
    AND cr.commodity_id = il.commodity_id 
    AND cr.date_id = il.date_id;