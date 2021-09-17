# Data Definition Language(DDL)

CREATE schema events_schema;

CREATE TABLE events_schema.tickets 
(
     ticket_id integer NOT NULL, 
     order_id integer NOT NULL,
     customer_id integer NOT NULL,
     quantity integer NOT NULL,
     Net_sales integer NOT NULL,
     event_code character varying COLLATE pg_catalog."default" NOT NULL,
     event_name character varying COLLATE pg_catalog."default" NOT NULL,
     event_season character varying COLLATE pg_catalog."default" NOT NULL,
     Date date  NULL,
     CONSTRAINT tickets_pkey PRIMARY KEY (ticket_id)
);

INSERT INTO events_schema.tickets(
     ticket_id, 
     order_id, 
     customer_id, 
     quantity, 
     Net_sales, 
     event_code, 
     event_name, 
     event_season, 
     Date)
VALUES ( 1, 182, 160, 1, 58.55, 'CHL-ARS', 'Chelsea vs Arsenal', '2020/2021', '2018-05-05');


# Query to find specific configuration information
-- select name, setting, unit, context, sourcefile, pending_restart
-- from pg_settings
-- where name ~ 'log.*connections';

-- select distinct context from pg_settings;