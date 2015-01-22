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

DROP TYPE IF EXISTS grid_join_point CASCADE;
CREATE TYPE grid_join_point AS (
    i     int4,
    j     int4,
    ratio float8
);

DROP TYPE IF EXISTS grid_join_point_big CASCADE;
CREATE TYPE grid_join_point_big AS (
    i     int8,
    j     int8,
    ratio float8
);

CREATE OR REPLACE FUNCTION hvault_grid_join(
    geometry, float8, float8, float8 = 0, float8 = 0)
    RETURNS SETOF grid_join_point_big
    AS 'MODULE_PATHNAME','hvault_grid_join'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION hvault_grid_join_area(
    geometry, int, int, float8, float8, float8, float8)
    RETURNS SETOF grid_join_point
    AS 'MODULE_PATHNAME','hvault_grid_join_area'
    LANGUAGE C IMMUTABLE STRICT;
