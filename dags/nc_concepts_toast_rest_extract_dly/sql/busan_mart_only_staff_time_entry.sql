DROP TABLE IF EXISTS staff_time_entry;

CREATE TABLE staff_time_entry
SELECT * 
FROM nc_concepts.staff_time_entry
WHERE business_name LIKE 'Busan Mart%'
;
