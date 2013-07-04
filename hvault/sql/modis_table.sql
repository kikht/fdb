CREATE FOREIGN TABLE modis1km (
    file_id   int4      OPTIONS (type 'catalog', cat_name 'id'),
    starttime timestamp OPTIONS (type 'catalog', cat_name 'starttime'),
    stoptime  timestamp OPTIONS (type 'catalog', cat_name 'stoptime'),

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

-- MOD09 layers
    Surface_Reflectance_Band_1        float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 1'),
    Surface_Reflectance_Band_2        float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 2'),
    Surface_Reflectance_Band_3        float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 3'),
    Surface_Reflectance_Band_4        float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 4'),
    Surface_Reflectance_Band_5        float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 5'),
    Surface_Reflectance_Band_6        float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 6'),
    Surface_Reflectance_Band_7        float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 7'),
    Surface_Reflectance_Band_8        float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 8'),
    Surface_Reflectance_Band_9        float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 9'),
    Surface_Reflectance_Band_10       float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 10'),
    Surface_Reflectance_Band_11       float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 11'),
    Surface_Reflectance_Band_12       float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 12'),
    Surface_Reflectance_Band_13       float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 13'),
    Surface_Reflectance_Band_14       float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 14'),
    Surface_Reflectance_Band_15       float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 15'),
    Surface_Reflectance_Band_16       float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 16'),

    Reflectance_Band_Quality          int4    OPTIONS (cat_name 'mod09', dataset '1km Reflectance Band Quality'),
    Reflectance_Data_State_QA         int2    OPTIONS (cat_name 'mod09', dataset '1km Reflectance Data State QA'),

    Atmospheric_Optical_Depth_Band_1  float8  OPTIONS (cat_name 'mod09', dataset '1km Atmospheric Optical Depth Band 1'),
    Atmospheric_Optical_Depth_Band_3  float8  OPTIONS (cat_name 'mod09', dataset '1km Atmospheric Optical Depth Band 3'),
    Atmospheric_Optical_Depth_Band_8  float8  OPTIONS (cat_name 'mod09', dataset '1km Atmospheric Optical Depth Band 8'),
    
    Atmospheric_Optical_Depth_Model   int2    OPTIONS (cat_name 'mod09', dataset '1km Atmospheric Optical Depth Model'),
    Atmospheric_Optical_Depth_Band_QA int2    OPTIONS (cat_name 'mod09', dataset '1km Atmospheric Optical Depth Band QA'),
    Atmospheric_Optical_Depth_Band_CM int2    OPTIONS (cat_name 'mod09', dataset '1km Atmospheric Optical Depth Band CM'),

    Band_3_Path_Radiance              float8  OPTIONS (cat_name 'mod09', dataset '1km Band 3 Path Radiance')

-- MOD35 layers
    Cloud_Mask                        bit(48) OPTIONS (cat_name 'mod35', bitmap_type 'prefix',  bitmap_dims '1', dataset 'Cloud_Mask'),
    Cloud_Mask_QA                     bit(80) OPTIONS (cat_name 'mod35', bitmap_type 'postfix', bitmap_dims '1', dataset 'Quality_Assurance'),

-- MOD04 layers
    Aerosol_Type_Land                             int2    OPTIONS (cat_name 'mod04', factor '10', dataset 'Aerosol_Type_Land'),
    Angstrom_Exponent_Land                        float8  OPTIONS (cat_name 'mod04', factor '10', dataset 'Angstrom_Exponent_Land'),
    Cloud_Fraction_Land                           float8  OPTIONS (cat_name 'mod04', factor '10', dataset 'Cloud_Fraction_Land'),
    Cloud_Fraction_Ocean                          float8  OPTIONS (cat_name 'mod04', factor '10', dataset 'Cloud_Fraction_Ocean'),
    Corrected_Optical_Depth_Land_wav2p1           float8  OPTIONS (cat_name 'mod04', factor '10', dataset 'Corrected_Optical_Depth_Land_wav2p1'),
    Deep_Blue_Aerosol_Optical_Depth_550_Land      float8  OPTIONS (cat_name 'mod04', factor '10', dataset 'Deep_Blue_Aerosol_Optical_Depth_550_Land'),
    Deep_Blue_Aerosol_Optical_Depth_550_Land_STD  float8  OPTIONS (cat_name 'mod04', factor '10', dataset 'Deep_Blue_Aerosol_Optical_Depth_550_Land_STD'),
    Deep_Blue_Angstrom_Exponent_Land              float8  OPTIONS (cat_name 'mod04', factor '10', dataset 'Deep_Blue_Angstrom_Exponent_Land'),
    Fitting_Error_Land                            float8  OPTIONS (cat_name 'mod04', factor '10', dataset 'Fitting_Error_Land'),
    Image_Optical_Depth_Land_And_Ocean            float8  OPTIONS (cat_name 'mod04', factor '10', dataset 'Image_Optical_Depth_Land_And_Ocean'),
    Mass_Concentration_Land                       float8  OPTIONS (cat_name 'mod04', factor '10', dataset 'Mass_Concentration_Land'),
    Number_Pixels_Used_Ocean                      int2    OPTIONS (cat_name 'mod04', factor '10', dataset 'Number_Pixels_Used_Ocean'),
    Optical_Depth_Land_And_Ocean                  float8  OPTIONS (cat_name 'mod04', factor '10', dataset 'Optical_Depth_Land_And_Ocean'),
    Optical_Depth_Ratio_Small_Land                float8  OPTIONS (cat_name 'mod04', factor '10', dataset 'Optical_Depth_Ratio_Small_Land'),
    Optical_Depth_Ratio_Small_Land_And_Ocean      float8  OPTIONS (cat_name 'mod04', factor '10', dataset 'Optical_Depth_Ratio_Small_Land_And_Ocean'),
    Scattering_Angle                              float8  OPTIONS (cat_name 'mod04', factor '10', dataset 'Scattering_Angle'),
    Quality_Assurance_Ocean                       bit(40) OPTIONS (cat_name 'mod04', factor '10', bitmap_type 'postfix', bitmap_dims '1', dataset 'Quality_Assurance_Ocean'),
    Quality_Assurance_Land                        bit(40) OPTIONS (cat_name 'mod04', factor '10', bitmap_type 'postfix', bitmap_dims '1', dataset 'Quality_Assurance_Land'),
    Quality_Assurance_Crit_Ref_Land               bit(40) OPTIONS (cat_name 'mod04', factor '10', bitmap_type 'postfix', bitmap_dims '1', dataset 'Quality_Assurance_Crit_Ref_Land'),
    -- TODO: lot more

-- MOD05 layers
    Water_Vapor_Near_Infrared                     float8  OPTIONS (cat_name 'mod05', dataset 'Water_Vapor_Near_Infrared'),
    Water_Vapor_Correction_Factors                int2    OPTIONS (cat_name 'mod05', dataset 'Water_Vapor_Correction_Factors'),
    Quality_Assurance_Near_Infrared               bit(8)  OPTIONS (cat_name 'mod05', bitmap_type 'postfix', bitmap_dims '1', dataset 'Quality_Assurance_Near_Infrared'),

    Water_Vapor_Infrared                          float8  OPTIONS (cat_name 'mod05', factor '5', dataset 'Water_Vapor_Infrared'),
    Quality_Assurance_Infrared                    bit(40) OPTIONS (cat_name 'mod05', factor '5', bitmap_type 'postfix', bitmap_dims '1', dataset 'Quality_Assurance_Infrared'),

-- MOD07 layers
    K_Index                     float8  OPTIONS (cat_name 'mod07', factor '5', dataset 'K_Index'),
    Lifted_Index                float8  OPTIONS (cat_name 'mod07', factor '5', dataset 'Lifted_Index'),
    Surface_Elevation           float8  OPTIONS (cat_name 'mod07', factor '5', dataset 'Surface_Elevation'),
    Surface_Temperature         float8  OPTIONS (cat_name 'mod07', factor '5', dataset 'Surface_Temperature'),
    Total_Ozone                 float8  OPTIONS (cat_name 'mod07', factor '5', dataset 'Total_Ozone'),
    Total_Totals                float8  OPTIONS (cat_name 'mod07', factor '5', dataset 'Total_Totals'),
    Tropopause_Height           float8  OPTIONS (cat_name 'mod07', factor '5', dataset 'Tropopause_Height'),
    Water_Vapor                 float8  OPTIONS (cat_name 'mod07', factor '5', dataset 'Water_Vapor'),
    Water_Vapor_Direct          float8  OPTIONS (cat_name 'mod07', factor '5', dataset 'Water_Vapor_Direct'),
    Water_Vapor_High            float8  OPTIONS (cat_name 'mod07', factor '5', dataset 'Water_Vapor_High'),
    Water_Vapor_Low             float8  OPTIONS (cat_name 'mod07', factor '5', dataset 'Water_Vapor_Low'),
    Surface_Pressure            float8  OPTIONS (cat_name 'mod07', factor '5', dataset 'Surface_Pressure'),

    Processing_Flag             int2    OPTIONS (cat_name 'mod07', factor '5', dataset 'Processing_Flag'),
    Quality_Assurance           bit(80) OPTIONS (cat_name 'mod07', factor '5', bitmap_type 'postfix', bitmap_dims '1', dataset 'Quality_Assurance'),
    Quality_Assurance_Infrared  bit(40) OPTIONS (cat_name 'mod07', factor '5', bitmap_type 'postfix', bitmap_dims '1', dataset 'Quality_Assurance_Infrared')
    --TODO:
    -- Retrieved_Height_Profile (20, 306, 270)
    -- Guess_Temperature_Profile (20, 306, 270)
    -- Brightness_Temperature (12, 306, 270)
    -- Guess_Moisture_Profile (20, 306, 270)
    -- Retrieved_Moisture_Profile (20, 306, 270)
    -- Retrieved_Temperature_Profile (20, 306, 270)

) SERVER hvault_service
  OPTIONS (catalog 'catalog',
           shift_longitude 'true');

CREATE FOREIGN TABLE modis500m (
    file_id   int4      OPTIONS (type 'catalog', cat_name 'id'),
    starttime timestamp OPTIONS (type 'catalog', cat_name 'starttime'),
    stoptime  timestamp OPTIONS (type 'catalog', cat_name 'stoptime'),

    index     int4      OPTIONS (type 'index'),
    line_id   int4      OPTIONS (type 'line_index'),
    sample_id int4      OPTIONS (type 'sample_index'),
    
    point     geometry  OPTIONS (type 'point',     cat_name 'mod03', factor '2'),
    footprint geometry  OPTIONS (type 'footprint', cat_name 'mod03', factor '2'),

-- MOD10 layers
    Snow_Cover                       int2    OPTIONS (cat_name 'mod10', dataset 'Snow_Cover'),
    Fractional_Snow_Cover            int2    OPTIONS (cat_name 'mod10', dataset 'Fractional_Snow_Cover'),
    Snow_Cover_Pixel_QA              int2    OPTIONS (cat_name 'mod10', dataset 'Snow_Cover_Pixel_QA'),

-- MOD09 layers
    500m_Surface_Reflectance_Band_1  float8  OPTIONS (cat_name 'mod09', dataset '500m Surface Reflectance_Band 1'),
    500m_Surface_Reflectance_Band_2  float8  OPTIONS (cat_name 'mod09', dataset '500m Surface Reflectance_Band 2'),
    500m_Surface_Reflectance_Band_3  float8  OPTIONS (cat_name 'mod09', dataset '500m Surface Reflectance_Band 3'),
    500m_Surface_Reflectance_Band_4  float8  OPTIONS (cat_name 'mod09', dataset '500m Surface Reflectance_Band 4'),
    500m_Surface_Reflectance_Band_5  float8  OPTIONS (cat_name 'mod09', dataset '500m Surface Reflectance_Band 5'),
    500m_Surface_Reflectance_Band_6  float8  OPTIONS (cat_name 'mod09', dataset '500m Surface Reflectance_Band 6'),
    500m_Surface_Reflectance_Band_7  float8  OPTIONS (cat_name 'mod09', dataset '500m Surface Reflectance_Band 7'),
    500m_Reflectance Band Quality    int4    OPTIONS (cat_name 'mod09', dataset '500m Reflectance Band Quality')

) SERVER hvault_service
  OPTIONS (catalog 'catalog',
           shift_longitude 'true');
