CREATE FOREIGN TABLE modis1km_fire (
    file_id   int4      OPTIONS (type 'catalog', cat_name 'id'),
    starttime timestamp OPTIONS (type 'catalog', cat_name 'starttime'),
    stoptime  timestamp OPTIONS (type 'catalog', cat_name 'stoptime'),
	is_terra  boolean   OPTIONS (type 'catalog', cat_name 'is_terra'),

    index     int4      OPTIONS (type 'index'),
    line_id   int4      OPTIONS (type 'line_index'),
    sample_id int4      OPTIONS (type 'sample_index'),
    
    point     geometry  OPTIONS (type 'point', cat_name 'mod03'),
    footprint geometry  OPTIONS (type 'footprint', cat_name 'mod03'),

	-- MOD03 layers
    Height                            int2    OPTIONS (cat_name 'mod03', dataset 'Height'),
    SensorZenith                      float8  OPTIONS (cat_name 'mod03', dataset 'SensorZenith'),
    SensorAzimuth                     float8  OPTIONS (cat_name 'mod03', dataset 'SensorAzimuth'),
    Range                             float8  OPTIONS (cat_name 'mod03', dataset 'Range'),
    SolarZenith                       float8  OPTIONS (cat_name 'mod03', dataset 'SolarZenith'),
    SolarAzimuth                      float8  OPTIONS (cat_name 'mod03', dataset 'SolarAzimuth'),
    LandSeaMask                       int2    OPTIONS (cat_name 'mod03', dataset 'Land/SeaMask'),
    gflags                            int2    OPTIONS (cat_name 'mod03', dataset 'gflags'),

	--MOD14 layers
	fire_mask                         int2    OPTIONS (cat_name 'mod14', dataset 'fire mask'),
	fire_QA                           int4    OPTIONS (cat_name 'mod14', dataset 'algorithm QA')
	
) SERVER hvault_service
  OPTIONS (catalog 'laads_catalog_view',
           driver 'modis_swath',
           shift_longitude 'true', 
           startup_cost '10', 
		   file_read_cost '30000', 
		   byte_cost '0.0007');


