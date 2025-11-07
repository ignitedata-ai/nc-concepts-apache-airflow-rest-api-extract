DROP VIEW IF EXISTS vw_ticket_items;
DROP TABLE IF EXISTS ticket_items;

CREATE VIEW vw_ticket_items AS 
SELECT
    ocs.selection_id AS ticket_item_id,
    ocs.order_id AS ticket_id,
    o.restaurant_external_id AS business_id,
    o.restaurant_name AS business_name,
    o.restaurant_address AS business_address,
    ocs.displayName AS item_name,
    ocs.price AS item_price,
    ocs.tax AS item_tax_amount,
    ocs.price + ocs.tax AS item_total_amount,
    ocs.quantity AS item_quantity,
    o.openedDate - INTERVAL 7 HOUR AS ticket_opened_date,
    o.paidDate - INTERVAL 7 HOUR AS ticket_paid_date,
    o.closedDate - INTERVAL 7 HOUR AS ticket_closed_date
FROM ignitelens.order_check_selections ocs
LEFT JOIN ignitelens.orders o USING (order_id)
WHERE o.deleted = FALSE AND o.voidDate IS NULL AND o.createdInTestMode = 0
;

CREATE TABLE ticket_items AS 
SELECT *
FROM vw_ticket_items
LIMIT 0
;

INSERT INTO ticket_items 
SELECT *
FROM vw_ticket_items
;
