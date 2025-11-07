DROP TABLE IF EXISTS staff_shift_summarized;

SELECT MIN(time_entry_date), MAX(time_entry_date) FROM staff_time_entry;

-- Dependent on staff_time_entry table

CREATE TABLE staff_shift_summarized AS
WITH RECURSIVE dates_series AS (
    SELECT DATE('2024-09-01') AS dt
    UNION ALL
    SELECT DATE_ADD(dt, INTERVAL 1 DAY)
    FROM dates_series
    WHERE dt < '{{ execution_date.strftime("%Y-%m-%d") }}'
), dates_and_businesses AS (
    SELECT DISTINCT
        dt AS dt,
        business_id AS business_id,
        business_name AS business_name,
        business_address AS business_address,
        business_type AS business_type
    FROM staff_time_entry
    CROSS JOIN dates_series
), base_summarized AS (
    SELECT DISTINCT
        time_entry_date AS shift_date,
        business_id AS business_id,
        business_name AS business_name,
        business_address AS business_address,
        business_type AS business_type,
        SUM(total_hours) AS total_hours,
        SUM(total_wages) AS total_wages,
        SUM(CASE WHEN worked_morning_shift THEN 1 ELSE 0 END) AS morning_shift_count,
        SUM(CASE WHEN worked_evening_shift THEN 1 ELSE 0 END) AS evening_shift_count
    FROM staff_time_entry
    GROUP BY time_entry_date, business_id, business_name, business_address, business_type
)
SELECT 
    ds.dt AS shift_date,
    ds.business_id AS business_id,
    ds.business_name AS business_name,
    ds.business_address AS business_address,
    ds.business_type AS business_type,
    IFNULL(s.total_hours, 0) AS total_hours,
    IFNULL(s.total_wages, 0) AS total_wages,
    IFNULL(s.morning_shift_count, 0) AS morning_shift_count,
    IFNULL(s.evening_shift_count, 0) AS evening_shift_count,
    IFNULL(s.morning_shift_count + s.evening_shift_count, 0) AS total_shift_count,
    IF(ush.holiday_date IS NOT NULL, TRUE, FALSE) AS is_holiday
FROM dates_and_businesses ds
LEFT JOIN base_summarized s ON ds.dt = s.shift_date AND ds.business_id = s.business_id AND ds.business_name = s.business_name AND ds.business_address = s.business_address
LEFT JOIN us_holidays ush ON (ds.dt = ush.holiday_date)
ORDER BY ds.dt 
;
