DROP TABLE IF EXISTS ticket_items;

CREATE TABLE ticket_items
SELECT * 
FROM nc_concepts.ticket_items
WHERE business_name LIKE 'Busan Mart%'
;
