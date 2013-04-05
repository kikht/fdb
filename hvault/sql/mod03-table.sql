CREATE FOREIGN TABLE mod03 (
    file_id   int4        OPTIONS (type 'file_index'),
    line_id   int4        OPTIONS (type 'line_index'),
    sample_id int4        OPTIONS (type 'sample_index'),
    point     geometry    OPTIONS (type 'point'),
    footprint geometry    OPTIONS (type 'footprint'),
    time      timestamp   OPTIONS (type 'time'),

    Height          int2    OPTIONS (sds 'Height', type 'int2'),
    SensorZenith    float8  OPTIONS (sds 'SensorZenith'),
    SensorAzimuth   float8  OPTIONS (sds 'SensorAzimuth'),
    Range           float8  OPTIONS (sds 'Range'),
    SolarZenith     float8  OPTIONS (sds 'SolarZenith'),
    SolarAzimuth    float8  OPTIONS (sds 'SolarAzimuth'),
    LandSeaMask     int2    OPTIONS (sds 'Land/SeaMask', type 'byte'),
    gflags          int2    OPTIONS (sds 'gflags', type 'byte')
) SERVER hvault_service
  OPTIONS (catalog 'mod03_catalog',
           shift_longitude 'true');
