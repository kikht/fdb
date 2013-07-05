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
    EXECUTE 'CREATE INDEX ON '||quote_ident($1)||' using gist (footprint)';
END;
$$ LANGUAGE PLPGSQL;

SELECT hvault_create_catalog('hdf_catalog');

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
alter table hdf_catalog add column mod35 text;
update hdf_catalog set mod35 = '/home/kikht/Downloads/hdf/MOD35_L2.A2013110.0115.005.2013111064806.hdf' 
    where starttime = '2013-04-20 01:15:01.349555';
update hdf_catalog set mod35 = '/home/kikht/Downloads/hdf/MOD35_L2.A2013110.0250.005.2013111064046.hdf' 
    where starttime = '2013-04-20 02:50:00.159843';    
update hdf_catalog set filename = 'lalalalalala' where starttime = '2013-04-20 02:50:00.159843';

ANALYZE hdf_catalog;

CREATE FOREIGN TABLE test_catalog (
    file_id   int4        OPTIONS (type 'catalog', cat_name 'file_id'),
    time      timestamp   OPTIONS (type 'catalog', cat_name 'starttime'),

    index     int4        OPTIONS (type 'index'),
    line_id   int4        OPTIONS (type 'line_index'),
    sample_id int4        OPTIONS (type 'sample_index'),

    
    point     geometry    OPTIONS (type 'point', cat_name 'filename'),
    footprint geometry    OPTIONS (type 'footprint', cat_name 'filename'),

    Surface_Reflectance_Band_1   float8  
        OPTIONS (type 'dataset', cat_name 'filename', 
                 dataset '1km Surface Reflectance Band 1'),
    Surface_Reflectance_Band_2   float8  
        OPTIONS (type 'dataset', cat_name 'filename', 
                 dataset '1km Surface Reflectance Band 2'),
    Surface_Reflectance_Band_3   float8  
        OPTIONS (type 'dataset', cat_name 'filename', 
                 dataset '1km Surface Reflectance Band 3'),
    Surface_Reflectance_Band_4   float8  
        OPTIONS (type 'dataset', cat_name 'filename', 
                 dataset '1km Surface Reflectance Band 4'),
    Surface_Reflectance_Band_5   float8  
        OPTIONS (type 'dataset', cat_name 'filename', 
                 dataset '1km Surface Reflectance Band 5'),
    Surface_Reflectance_Band_6   float8  
        OPTIONS (type 'dataset', cat_name 'filename', 
                 dataset '1km Surface Reflectance Band 6'),
    Surface_Reflectance_Band_7   float8  
        OPTIONS (type 'dataset', cat_name 'filename', 
                 dataset '1km Surface Reflectance Band 7'),
    Surface_Reflectance_Band_8   float8  
        OPTIONS (type 'dataset', cat_name 'filename', 
                 dataset '1km Surface Reflectance Band 8'),
    Surface_Reflectance_Band_9   float8  
        OPTIONS (type 'dataset', cat_name 'filename', 
                 dataset '1km Surface Reflectance Band 9'),
    Surface_Reflectance_Band_10  float8  
        OPTIONS (type 'dataset', cat_name 'filename', 
                 dataset '1km Surface Reflectance Band 10'),
    Surface_Reflectance_Band_11  float8  
        OPTIONS (type 'dataset', cat_name 'filename', 
                 dataset '1km Surface Reflectance Band 11'),
    Surface_Reflectance_Band_12  float8  
        OPTIONS (type 'dataset', cat_name 'filename', 
                 dataset '1km Surface Reflectance Band 12'),
    Surface_Reflectance_Band_13  float8  
        OPTIONS (type 'dataset', cat_name 'filename', 
                 dataset '1km Surface Reflectance Band 13'),
    Surface_Reflectance_Band_14  float8  
        OPTIONS (type 'dataset', cat_name 'filename', 
                 dataset '1km Surface Reflectance Band 14'),
    Surface_Reflectance_Band_15  float8  
        OPTIONS (type 'dataset', cat_name 'filename', 
                 dataset '1km Surface Reflectance Band 15'),
    Surface_Reflectance_Band_16  float8  
        OPTIONS (type 'dataset', cat_name 'filename', 
                 dataset '1km Surface Reflectance Band 16'),

    Cloud_Mask                   bit(48)
        OPTIONS (type 'dataset', cat_name 'mod35', dataset 'Cloud_Mask',
                 bitmap_type 'prefix', bitmap_dims '1'),
    Cloud_Mask_1byte           int2
        OPTIONS (type 'dataset', cat_name 'mod35', dataset 'Cloud_Mask',
                 prefix '1'),
    Cloud_Mask_QA                bit(80)
        OPTIONS (type 'dataset', cat_name 'mod35', dataset 'Quality_Assurance',
                 bitmap_type 'postfix', bitmap_dims '1'),
    Reflectance_Band_Quality     int4    
        OPTIONS (type 'dataset', cat_name 'filename', 
                 dataset '1km Reflectance Band Quality')
) SERVER hvault_service
  OPTIONS (catalog 'hdf_catalog',
           shift_longitude 'true');
