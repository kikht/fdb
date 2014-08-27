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

DROP TYPE IF EXISTS modis_metadata CASCADE;
CREATE TYPE modis_metadata AS (
    footprint text,
    starttime timestamp,
    stoptime  timestamp
);

CREATE OR REPLACE FUNCTION hvault_modis_metadata(file text) 
    RETURNS modis_metadata AS 
$$        
    import gdal
    gd = gdal.Open(file)
    if gd == None:
        return None
    
    coord = dict()
    coord["north"] = float(gd.GetMetadataItem("NORTHBOUNDINGCOORDINATE"))
    if not coord["north"]:
        return None
    coord["south"] = float(gd.GetMetadataItem("SOUTHBOUNDINGCOORDINATE"))
    if not coord["south"]:
        return None
    coord["east"] = float(gd.GetMetadataItem("EASTBOUNDINGCOORDINATE"))
    if not coord["east"]:
        return None
    coord["west"] = float(gd.GetMetadataItem("WESTBOUNDINGCOORDINATE"))
    if not coord["west"]:
        return None
    if coord["east"] < 0:
        coord["east"] += 360.0
    if coord["west"] < 0:
        coord["east"] += 360.0

    startdate = gd.GetMetadataItem("RANGEBEGINNINGDATE")
    if not startdate:
        return None
    starttime = gd.GetMetadataItem("RANGEBEGINNINGTIME")
    if not starttime:
        return None
    stopdate  = gd.GetMetadataItem("RANGEENDINGDATE")
    if not stopdate:
        return None
    stoptime  = gd.GetMetadataItem("RANGEENDINGTIME")
    if not stoptime:
        return None

    footprint = "POLYGON(( {west} {north}, {east} {north}, {east} {south}, {west} {south}, {west} {north} ))".format(**coord)
    start = startdate + " " + starttime
    stop  = stopdate  + " " + stoptime
    return (footprint, start, stop)
$$ LANGUAGE plpythonu STABLE STRICT COST 10000;

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

CREATE OR REPLACE FUNCTION hvault_table_count_step(
    int[], int[], int[][]) RETURNS int[]
    AS 'MODULE_PATHNAME','hvault_table_count_step'
    LANGUAGE C IMMUTABLE;

CREATE AGGREGATE hvault_table_count( int[], int[][] ) (
    SFUNC = hvault_table_count_step,
    STYPE = int[]
);
    
