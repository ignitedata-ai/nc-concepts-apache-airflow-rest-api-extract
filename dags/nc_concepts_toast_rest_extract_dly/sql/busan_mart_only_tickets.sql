DROP TABLE IF EXISTS tickets;

CREATE TABLE tickets
SELECT * 
FROM nc_concepts.tickets
WHERE business_name LIKE 'Busan Mart%'
;
