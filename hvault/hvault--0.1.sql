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
