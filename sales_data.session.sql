SELECT MAX(LENGTH(CAST(card_number AS text))) AS max_length
FROM orders_table;
