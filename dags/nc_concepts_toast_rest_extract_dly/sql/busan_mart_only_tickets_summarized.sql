DROP TABLE IF EXISTS tickets_summarized;

CREATE TABLE tickets_summarized
SELECT * 
FROM nc_concepts.tickets_summarized
WHERE business_name LIKE 'Busan Mart%'
;
