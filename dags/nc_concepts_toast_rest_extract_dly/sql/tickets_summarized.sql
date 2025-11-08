DROP TABLE IF EXISTS tickets_summarized;

-- Dependent on tickets and us_holidays tables

CREATE TABLE tickets_summarized AS
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
    FROM tickets
    CROSS JOIN dates_series
),
base_summarized AS (
    SELECT 
        DATE(opened_date) AS business_date,
        business_id AS business_id,
        business_name AS business_name,
        business_address AS business_address,
        business_type AS business_type,
        COUNT(ticket_id) AS ticket_count,
        SUM(CASE WHEN TIME(opened_date) BETWEEN '11:00:00' AND '14:00:00' THEN 1 ELSE 0 END) AS lunch_ticket_count,
        SUM(CASE WHEN TIME(opened_date) BETWEEN '17:00:00' AND '22:00:00' THEN 1 ELSE 0 END) AS dinner_ticket_count,
        AVG(total_amount) AS avg_ticket_amount,
        SUM(total_amount) AS total_ticket_amount,
        MIN(total_amount) AS min_ticket_amount,
        MAX(total_amount) AS max_ticket_amount
    FROM tickets
    GROUP BY DATE(opened_date), business_id, business_name, business_address, business_type
)
SELECT
    ds.dt AS business_date,
    ds.business_id AS business_id,
    ds.business_name AS business_name,
    ds.business_address AS business_address,
    ds.business_type AS business_type,
    IFNULL(s.ticket_count, 0) AS ticket_count,
    IFNULL(s.lunch_ticket_count, 0) AS lunch_ticket_count,
    IFNULL(s.dinner_ticket_count, 0) AS dinner_ticket_count,
    IFNULL(s.avg_ticket_amount, 0) AS avg_ticket_amount,
    IFNULL(s.total_ticket_amount, 0) AS total_ticket_amount,
    IFNULL(s.min_ticket_amount, 0) AS min_ticket_amount,
    IFNULL(s.max_ticket_amount, 0) AS max_ticket_amount,
    IF(ush.holiday_date IS NOT NULL, TRUE, FALSE) AS is_holiday
FROM dates_and_businesses ds
LEFT JOIN base_summarized s ON ds.dt = s.business_date AND ds.business_id = s.business_id AND ds.business_name = s.business_name AND ds.business_address = s.business_address AND ds.business_type = s.business_type
LEFT JOIN us_holidays ush ON (ds.dt = ush.holiday_date)
;
