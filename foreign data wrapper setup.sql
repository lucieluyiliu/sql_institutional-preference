
create extension postgres_fdw;
  schema public
  version '1.0';

comment on extension postgres_fdw is 'foreign-data wrapper for remote PostgreSQL servers';


SELECT usename FROM pg_user;

CREATE SERVER foreign_server
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host 'wrds-pgdata.wharton.upenn.edu', port '9737', dbname 'wrds');

-- auto-generated definition
create user mapping for postgres
  server foreign_server options (user 'yiliulu', password 'Wsw8dgnl');

--import foreign schema
CREATE SCHEMA comp_global;
IMPORT FOREIGN SCHEMA comp_global
  FROM SERVER foreign_server
  INTO comp_global;

CREATE SCHEMA factset;
IMPORT FOREIGN SCHEMA factset
  FROM SERVER foreign_server
  INTO factset;

