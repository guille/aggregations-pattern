-- Create "source" DB
CREATE DATABASE source;
\connect source;

CREATE TABLE invoices (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    total NUMERIC(20,2) NOT NULL
);

-- Create "target" DB
CREATE DATABASE target;
\connect target;

CREATE TABLE cdc_entries (
    invoice_id SERIAL PRIMARY KEY,
    total NUMERIC(20,2) NOT NULL,
    lsn BIGINT NOT NULL
);

CREATE TABLE aggregates (
    total NUMERIC(20,2) NOT NULL
);
-- We're just having a single row here, code assumes it exists
INSERT INTO aggregates (total) VALUES (0);
