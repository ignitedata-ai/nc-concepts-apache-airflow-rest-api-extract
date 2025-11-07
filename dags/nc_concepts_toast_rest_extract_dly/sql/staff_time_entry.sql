DROP TABLE IF EXISTS staff_time_entry;

CREATE TABLE staff_time_entry AS
SELECT DISTINCT
    te.time_entry_id AS staff_time_entry_id,
    te.employee_id AS staff_id,
    te.job_id AS role_id,
    te.businessDate AS time_entry_date,
    clock_in,
    clock_out,
    te.regularHours AS total_hours,
    te.hourlyWage AS hourly_wage,
    te.regularHours * te.hourlyWage AS total_wages,
    -- Morning shift: overlap between 11:00-15:00
    CASE 
        WHEN FLOOR(regularHours) != 0 
         AND (
            clock_in BETWEEN DATE_ADD(DATE(clock_in), INTERVAL 0 HOUR) AND DATE_ADD(DATE(clock_in), INTERVAL 12 HOUR)
            OR 
            clock_out BETWEEN DATE_ADD(DATE(clock_out), INTERVAL 13 HOUR) AND DATE_ADD(DATE(clock_out), INTERVAL 16 HOUR)
         )
        THEN TRUE
        ELSE FALSE
    END AS worked_morning_shift,
    -- Evening shift: overlap between 15:00-22:00
    CASE 
        WHEN FLOOR(regularHours) != 0 
         AND (
            clock_in BETWEEN DATE_ADD(DATE(clock_in), INTERVAL 14 HOUR) AND DATE_ADD(DATE(clock_in), INTERVAL 15 HOUR)
            OR 
            clock_out BETWEEN DATE_ADD(DATE(clock_out), INTERVAL 20 HOUR) AND DATE_ADD(DATE(clock_out), INTERVAL 24 HOUR)
            OR 
            DATE(clock_out) = DATE(DATE_ADD(clock_in, INTERVAL 1 DAY))  -- or is next day
         )
        THEN TRUE
        ELSE FALSE
    END AS worked_evening_shift,
    IF(ush.holiday_date IS NOT NULL, TRUE, FALSE) AS is_holiday,
    te.restaurant_external_id AS business_id,
    te.restaurant_name AS business_name,
    te.restaurant_address AS business_address,
    IF(LOWER(te.restaurant_name) LIKE '%busan mart%', "GROCERY", "RESTAURANT")  AS business_type
FROM (
    SELECT 
        *,
        inDate - INTERVAL 7 HOUR AS clock_in,
        outDate - INTERVAL 7 HOUR AS clock_out
    FROM ignitelens.time_entries
) te
LEFT JOIN us_holidays ush ON (te.businessDate = ush.holiday_date)
WHERE deleted = 0
;

ALTER TABLE staff_time_entry
ADD PRIMARY KEY (staff_time_entry_id)
;
