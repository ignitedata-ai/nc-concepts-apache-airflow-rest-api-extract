DROP TABLE IF EXISTS staff_shift_summarized;

CREATE TABLE staff_shift_summarized
SELECT * 
FROM nc_concepts.staff_shift_summarized
WHERE business_name LIKE 'Busan Mart%'
;
