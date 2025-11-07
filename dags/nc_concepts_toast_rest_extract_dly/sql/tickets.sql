DROP TABLE IF EXISTS tickets;

CREATE TABLE tickets AS 
SELECT DISTINCT
    o.order_id AS ticket_id,
    o.restaurant_external_id AS business_id,
    o.restaurant_name AS business_name,
    o.restaurant_address AS business_address,
    IF(LOWER(o.restaurant_name) LIKE '%busan mart%', "GROCERY", "RESTAURANT")  AS business_type,
    o.source AS order_type,
    o.approvalStatus AS approval_status,
    o.server_id AS employee_server_id,
    o.numberOfGuests AS number_of_guests,
    oc.amount AS amount,
    oc.taxAmount AS tax_amount,
    oc.totalAmount AS total_amount,
    o.openedDate - INTERVAL 7 HOUR AS opened_date,
    o.paidDate - INTERVAL 7 HOUR AS paid_date,
    o.closedDate - INTERVAL 7 HOUR AS closed_date
FROM ignitelens.orders o
LEFT JOIN ignitelens.order_checks oc USING (order_id)
WHERE o.deleted = FALSE AND o.voidDate IS NULL AND o.createdInTestMode = 0
;

-- Can't do this since there are some duplicate ticket_ids from multiple checks associated with the same order
-- ALTER TABLE tickets
-- ADD PRIMARY KEY (ticket_id)
-- ;
