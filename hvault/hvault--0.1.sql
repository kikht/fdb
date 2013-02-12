create or replace function hvault_fdw_validator (text[], oid) 
    returns bool 
    as 'MODULE_PATHNAME' 
    language C strict;

create or replace function hvault_fdw_handler() 
    returns fdw_handler
    as 'MODULE_PATHNAME' 
    language C strict;

create foreign data wrapper hvault_fdw
    validator hvault_fdw_validator
	handler   hvault_fdw_handler;

create server hvault_service foreign data wrapper hvault_fdw;

-- create foreign table test_foreign (
--     num    int4,
--     str    text,
--     numf   double precision,
--     gmtr   geometry,
--     dt     timestamp
-- ) server hvault_service;

CREATE FOREIGN TABLE test_hdf (
    file_id   int4        OPTIONS (type 'file_index'),
    line_id   int4        OPTIONS (type 'line_index'),
    sample_id int4        OPTIONS (type 'sample_index'),
    pnt       geometry    OPTIONS (type 'point'),
    footpt    geometry    OPTIONS (type 'footprint'),
--  tstamp    timestamptz OPTIONS (type 'time'),
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
  OPTIONS (filename '/home/kikht/Downloads/MOD09.A2013026.0635.005.2013027075743.hdf')

