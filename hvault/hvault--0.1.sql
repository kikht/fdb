CREATE OR REPLACE FUNCTION hvault_fdw_validator (text[], oid) 
    RETURNS bool 
    AS 'MODULE_PATHNAME' 
    LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION hvault_fdw_handler() 
    RETURNS fdw_handler
    AS 'MODULE_PATHNAME' 
    LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER hvault_fdw
    VALIDATOR hvault_fdw_validator
	HANDLER   hvault_fdw_handler;

CREATE SERVER hvault_service FOREIGN DATA WRAPPER hvault_fdw;

CREATE OR REPLACE FUNCTION hvault_load_modis_swath (cstring, cstring)
    RETURNS bool
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION hvault_mass_load_modis_swath 
    (cstring, cstring, cstring)
    RETURNS int4
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION hvault_create_catalog(name text) RETURNS void AS $$
BEGIN
	EXECUTE 'CREATE TABLE '||quote_ident($1)||' (
		file_id   serial    PRIMARY KEY,
		filename  text      UNIQUE NOT NULL,
		starttime timestamp NOT NULL,
		stoptime  timestamp NOT NULL,
		footprint geometry  NOT NULL,
		size      int8      NOT NULL
	)';
	EXECUTE 'CREATE INDEX ON '||quote_ident($1)||' (starttime)';
END;
$$ LANGUAGE PLPGSQL;
