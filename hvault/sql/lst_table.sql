CREATE FOREIGN TABLE lst_terra_daily (
    file_id   int4      OPTIONS (type 'catalog', cat_name 'id'),
    starttime timestamp OPTIONS (type 'catalog', cat_name 'starttime'),
    stoptime  timestamp OPTIONS (type 'catalog', cat_name 'stoptime'),

    index     int4      OPTIONS (type 'index'),
    line_id   int4      OPTIONS (type 'line_index'),
    sample_id int4      OPTIONS (type 'sample_index'),
    
    point     geometry  OPTIONS (type 'point', cat_name 'tile'),
    footprint geometry  OPTIONS (type 'footprint', cat_name 'tile'),

    LST_day          float8 OPTIONS (cat_name 'mod11a1', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_Daily_1km_LST:LST_Day_1km'),
    QC_Day           int2   OPTIONS (cat_name 'mod11a1', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_Daily_1km_LST:QC_Day'),
    Day_view_time    float8 OPTIONS (cat_name 'mod11a1', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_Daily_1km_LST:Day_view_time'),
    Day_view_angl    float8 OPTIONS (cat_name 'mod11a1', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_Daily_1km_LST:Day_view_angl'),
    LST_Night        float8 OPTIONS (cat_name 'mod11a1', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_Daily_1km_LST:LST_Night_1km'),
    QC_Night         int2   OPTIONS (cat_name 'mod11a1', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_Daily_1km_LST:QC_Night'),
    Night_view_time  float8 OPTIONS (cat_name 'mod11a1', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_Daily_1km_LST:Night_view_time'),
    Night_view_angl  float8 OPTIONS (cat_name 'mod11a1', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_Daily_1km_LST:Night_view_angl'),
    Emis_31          float8 OPTIONS (cat_name 'mod11a1', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_Daily_1km_LST:Emis_31'),
    Emis_32          float8 OPTIONS (cat_name 'mod11a1', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_Daily_1km_LST:Emis_32'),
    Clear_day_cov    float8 OPTIONS (cat_name 'mod11a1', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_Daily_1km_LST:Clear_day_cov'),
    Clear_night_cov  float8 OPTIONS (cat_name 'mod11a1', dataset 'HDF4_EOS:EOS_GRID:"%f":MODIS_Grid_Daily_1km_LST:Clear_night_cov')
    
) SERVER hvault_service
  OPTIONS (catalog 'lst_catalog_terra',
           driver 'gdal',
           shift_longitude 'true');


