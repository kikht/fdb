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

-- CREATE FOREIGN TABLE test_single (
--     file_id   int4        OPTIONS (type 'file_index'),
--     line_id   int4        OPTIONS (type 'line_index'),
--     sample_id int4        OPTIONS (type 'sample_index'),
--     pnt       geometry    OPTIONS (type 'point'),
--     footpt    geometry    OPTIONS (type 'footprint'),
--     tstamp    timestamp   OPTIONS (type 'time'),
--     band1     float8      OPTIONS (sds  '1km Surface Reflectance Band 1'),
--     band2     float8      OPTIONS (sds  '1km Surface Reflectance Band 2'),
--     band3     float8      OPTIONS (sds  '1km Surface Reflectance Band 3'),
--     band4     float8      OPTIONS (sds  '1km Surface Reflectance Band 4'),
--     band5     float8      OPTIONS (sds  '1km Surface Reflectance Band 5'),
--     band6     float8      OPTIONS (sds  '1km Surface Reflectance Band 6'),
--     band7     float8      OPTIONS (sds  '1km Surface Reflectance Band 7'),
--     band8     float8      OPTIONS (sds  '1km Surface Reflectance Band 8'),
--     band9     float8      OPTIONS (sds  '1km Surface Reflectance Band 9'),
--     band10    float8      OPTIONS (sds  '1km Surface Reflectance Band 10'),
--     band11    float8      OPTIONS (sds  '1km Surface Reflectance Band 11'),
--     band12    float8      OPTIONS (sds  '1km Surface Reflectance Band 12'),
--     band13    float8      OPTIONS (sds  '1km Surface Reflectance Band 13'),
--     band14    float8      OPTIONS (sds  '1km Surface Reflectance Band 14'),
--     band15    float8      OPTIONS (sds  '1km Surface Reflectance Band 15'),
--     band16    float8      OPTIONS (sds  '1km Surface Reflectance Band 16')
-- ) SERVER hvault_service
--   OPTIONS (filename '/home/kikht/Downloads/MOD09.A2013026.0635.005.2013027075743.hdf');

CREATE TABLE hdf_catalog (
    file_id   serial    PRIMARY KEY,
    filename  text      UNIQUE NOT NULL,
    starttime timestamp NOT NULL,
    stoptime  timestamp NOT NULL,
    footprint geometry  NOT NULL,
    size      int8      NOT NULL
);
CREATE INDEX ON hdf_catalog (starttime);

-- SELECT hvault_load_modis_swath(
--     'hdf_catalog',
--     '/home/kikht/Downloads/MOD09.A2013026.0635.005.2013027075743.hdf');
-- SELECT hvault_load_modis_swath(
--     'hdf_catalog',
--     '/home/kikht/Downloads/MOD09.A2013027.0540.005.2013028080139.hdf');
-- SELECT hvault_load_modis_swath(
--     'hdf_catalog',
--     '/home/kikht/Downloads/MOD09.A2013027.0536.005.2013028080029.hdf');

SELECT hvault_mass_load_modis_swath(
    'hdf_catalog', '/home/kikht/Downloads', 'MOD09*.hdf');

CREATE FOREIGN TABLE test_catalog (
    file_id   int4        OPTIONS (type 'file_index'),
    line_id   int4        OPTIONS (type 'line_index'),
    sample_id int4        OPTIONS (type 'sample_index'),
    pnt       geometry    OPTIONS (type 'point'),
    footpt    geometry    OPTIONS (type 'footprint'),
    tstamp    timestamp   OPTIONS (type 'time'),
    band1     float8      OPTIONS (sds  '1km Surface Reflectance Band 1'),
    band2     float8      OPTIONS (sds  '1km Surface Reflectance Band 2'),
    band3     float8      OPTIONS (sds  '1km Surface Reflectance Band 3'),
    band4     float8      OPTIONS (sds  '1km Surface Reflectance Band 4'),
    band5     float8      OPTIONS (sds  '1km Surface Reflectance Band 5'),
    band6     float8      OPTIONS (sds  '1km Surface Reflectance Band 6'),
    band7     float8      OPTIONS (sds  '1km Surface Reflectance Band 7'),
    band8     float8      OPTIONS (sds  '1km Surface Reflectance Band 8'),
    band9     float8      OPTIONS (sds  '1km Surface Reflectance Band 9'),
    band10    float8      OPTIONS (sds  '1km Surface Reflectance Band 10'),
    band11    float8      OPTIONS (sds  '1km Surface Reflectance Band 11'),
    band12    float8      OPTIONS (sds  '1km Surface Reflectance Band 12'),
    band13    float8      OPTIONS (sds  '1km Surface Reflectance Band 13'),
    band14    float8      OPTIONS (sds  '1km Surface Reflectance Band 14'),
    band15    float8      OPTIONS (sds  '1km Surface Reflectance Band 15'),
    band16    float8      OPTIONS (sds  '1km Surface Reflectance Band 16')
) SERVER hvault_service
  OPTIONS (catalog 'hdf_catalog');
