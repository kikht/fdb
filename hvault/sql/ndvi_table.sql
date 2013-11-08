CREATE FOREIGN TABLE ndvi (
    file_id   int4      OPTIONS (type 'catalog', cat_name 'id'),
    starttime timestamp OPTIONS (type 'catalog', cat_name 'starttime'),
    stoptime  timestamp OPTIONS (type 'catalog', cat_name 'stoptime'),

    index     int4      OPTIONS (type 'index'),
    line_id   int4      OPTIONS (type 'line_index'),
    sample_id int4      OPTIONS (type 'sample_index'),
    
    point     geometry  OPTIONS (type 'point', cat_name 'tile'),
    footprint geometry  OPTIONS (type 'footprint', cat_name 'tile'),

-- MOD13 layers
    NDVI                       float OPTIONS (cat_name 'mod13q1', inverse_scale 'true', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_16DAY_250m_500m_VI:250m 16 days NDVI'), 
    EVI                        float OPTIONS (cat_name 'mod13q1', inverse_scale 'true', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_16DAY_250m_500m_VI:250m 16 days EVI'), 
    VI_Quality                 int2  OPTIONS (cat_name 'mod13q1', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_16DAY_250m_500m_VI:250m 16 days VI Quality'),
    red_reflectance            float OPTIONS (cat_name 'mod13q1', inverse_scale 'true', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_16DAY_250m_500m_VI:250m 16 days red reflectance'),
    NIR_reflectance            float OPTIONS (cat_name 'mod13q1', inverse_scale 'true', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_16DAY_250m_500m_VI:250m 16 days NIR reflectance'),
    blue_reflectance           float OPTIONS (cat_name 'mod13q1', inverse_scale 'true', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_16DAY_250m_500m_VI:250m 16 days blue reflectance'), 
    MIR_reflectance            float OPTIONS (cat_name 'mod13q1', inverse_scale 'true', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_16DAY_250m_500m_VI:250m 16 days MIR reflectance'),
    view_zenith_angle          float OPTIONS (cat_name 'mod13q1', inverse_scale 'true', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_16DAY_250m_500m_VI:250m 16 days view zenith angle'),
    sun_zenith_angle           float OPTIONS (cat_name 'mod13q1', inverse_scale 'true', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_16DAY_250m_500m_VI:250m 16 days sun zenith angle'),
    relative_azimuth_angle     float OPTIONS (cat_name 'mod13q1', inverse_scale 'true', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_16DAY_250m_500m_VI:250m 16 days relative azimuth angle'),
    composite_day_of_the_year  int2  OPTIONS (cat_name 'mod13q1', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_16DAY_250m_500m_VI:250m 16 days composite day of the year'),
    pixel_reliability          int2  OPTIONS (cat_name 'mod13q1', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_16DAY_250m_500m_VI:250m 16 days pixel reliability')
) SERVER hvault_service
  OPTIONS (catalog 'gdal_catalog',
           driver 'gdal',
           shift_longitude 'true');

