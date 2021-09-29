
CREATE TABLE IF NOT EXISTS tickets
(
     ticket_id integer NULL, 
     order_id integer NULL,
     customer_id integer NULL,
     quantity integer NULL,
     net_sales integer NULL,
     event_code VARCHAR NULL,
     event_name VARCHAR NULL,
     event_season VARCHAR NULL,
     Date date  NULL
);

INSERT INTO tickets(ticket_id, order_id, customer_id, 
quantity, net_sales, event_code, event_name, event_season, Date )
VALUES( 1, 182, 160, 1, 58.55, 'CHL-ARS', 
   'Chelsea vs Arsenal', '2020/2021', '2018-05-05');


-- # Query to find specific configuration information
-- select name, setting, unit, context, sourcefile, pendin