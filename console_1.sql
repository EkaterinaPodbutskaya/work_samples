--SET search_path TO  first_schema;

CREATE TEMPORARY TABLE temp_table_name AS
SELECT c.company_name, e.first_name || ' ' || e.last_name AS name_lastname
FROM customers AS c
JOIN orders AS o USING (customer_id)
JOIN employees AS e USING (employee_id)
JOIN shippers AS s ON o.ship_via = s.shipper_id
WHERE c.city = 'London'
  AND c.city = e.city
  AND s.company_name = 'Speedy Express';


SELECT *
FROM temp_table_name;

DROP TABLE IF EXISTS temp_table_name;


SELECT company_name, contact_name
FROM customers
WHERE EXISTS(SELECT 1
             FROM orders
             WHERE orders.customer_id = customers.customer_id
               AND freight BETWEEN 50 AND 100);



SELECT distinct c.company_name, c.contact_name
FROM customers AS c, orders AS o
WHERE o.customer_id = c.customer_id
    AND freight BETWEEN 50 AND 100;

