CREATE FOREIGN TABLE mod09 (
    file_id   int4        OPTIONS (type 'file_index'),
    line_id   int4        OPTIONS (type 'line_index'),
    sample_id int4        OPTIONS (type 'sample_index'),
    point     geometry    OPTIONS (type 'point'),
    footprint geometry    OPTIONS (type 'footprint'),
    time      timestamp   OPTIONS (type 'time'),

    Atmospheric_Optical_Depth_Band_1    float8  OPTIONS (sds '1km Atmospheric Optical Depth Band 1'),
    Atmospheric_Optical_Depth_Band_3    float8  OPTIONS (sds '1km Atmospheric Optical Depth Band 3'),
    Atmospheric_Optical_Depth_Band_8    float8  OPTIONS (sds '1km Atmospheric Optical Depth Band 8'),

    Atmospheric_Optical_Depth_Model     byte    OPTIONS (sds '1km Atmospheric Optical Depth Model', type 'byte'),
    Atmospheric_Optical_Depth_Band_QA   int2    OPTIONS (sds '1km Atmospheric Optical Depth Band QA', type 'int2'),
    Atmospheric_Optical_Depth_Band_CM   byte    OPTIONS (sds '1km Atmospheric Optical Depth Band CM', type 'byte'),

    Surface_Reflectance_Band_1          float8  OPTIONS (sds '1km Surface Reflectance Band 1'),
    Surface_Reflectance_Band_2          float8  OPTIONS (sds '1km Surface Reflectance Band 2'),
    Surface_Reflectance_Band_3          float8  OPTIONS (sds '1km Surface Reflectance Band 3'),
    Surface_Reflectance_Band_4          float8  OPTIONS (sds '1km Surface Reflectance Band 4'),
    Surface_Reflectance_Band_5          float8  OPTIONS (sds '1km Surface Reflectance Band 5'),
    Surface_Reflectance_Band_6          float8  OPTIONS (sds '1km Surface Reflectance Band 6'),
    Surface_Reflectance_Band_7          float8  OPTIONS (sds '1km Surface Reflectance Band 7'),
    Surface_Reflectance_Band_8          float8  OPTIONS (sds '1km Surface Reflectance Band 8'),
    Surface_Reflectance_Band_9          float8  OPTIONS (sds '1km Surface Reflectance Band 9'),
    Surface_Reflectance_Band_10         float8  OPTIONS (sds '1km Surface Reflectance Band 10'),
    Surface_Reflectance_Band_11         float8  OPTIONS (sds '1km Surface Reflectance Band 11'),
    Surface_Reflectance_Band_12         float8  OPTIONS (sds '1km Surface Reflectance Band 12'),
    Surface_Reflectance_Band_13         float8  OPTIONS (sds '1km Surface Reflectance Band 13'),
    Surface_Reflectance_Band_14         float8  OPTIONS (sds '1km Surface Reflectance Band 14'),
    Surface_Reflectance_Band_15         float8  OPTIONS (sds '1km Surface Reflectance Band 15'),
    Surface_Reflectance_Band_16         float8  OPTIONS (sds '1km Surface Reflectance Band 16'),
    Surface_Reflectance_Band_26         float8  OPTIONS (sds '1km Surface Reflectance Band 26'),
    
    Reflectance_Band_Quality            int4    OPTIONS (sds '1km Reflectance Band Quality', type int4),
    Reflectance_Data_State_QA           int2    OPTIONS (sds '1km Reflectance Data State QA', type int2),
    
    Band_3_Path_Radiance                float8  OPTIONS (sds '1km Band 3 Path Radiance')
) SERVER hvault_service
  OPTIONS (catalog 'mod09_catalog',
           shift_longitude 'true');
