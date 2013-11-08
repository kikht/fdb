CREATE FOREIGN TABLE modis1km (
    file_id   int4      OPTIONS (type 'catalog', cat_name 'id'),
    starttime timestamp OPTIONS (type 'catalog', cat_name 'starttime'),
    stoptime  timestamp OPTIONS (type 'catalog', cat_name 'stoptime'),

    index     int4      OPTIONS (type 'index'),
    line_id   int4      OPTIONS (type 'line_index'),
    sample_id int4      OPTIONS (type 'sample_index'),
    
    point     geometry  OPTIONS (type 'point', cat_name 'mod03'),
    footprint geometry  OPTIONS (type 'footprint', cat_name 'mod03'),

-- MOD02 layers
    EV_Reflective_Band_1                 float8  OPTIONS (cat_name 'mod021km', dataset 'EV_250_Aggr1km_RefSB', prefix '0', scale 'reflectance_scales', offset 'reflectance_offsets'),
    EV_Reflective_Band_2                 float8  OPTIONS (cat_name 'mod021km', dataset 'EV_250_Aggr1km_RefSB', prefix '1', scale 'reflectance_scales', offset 'reflectance_offsets'),
    EV_Reflective_Band_3                 float8  OPTIONS (cat_name 'mod021km', dataset 'EV_500_Aggr1km_RefSB', prefix '0', scale 'reflectance_scales', offset 'reflectance_offsets'),
    EV_Reflective_Band_4                 float8  OPTIONS (cat_name 'mod021km', dataset 'EV_500_Aggr1km_RefSB', prefix '1', scale 'reflectance_scales', offset 'reflectance_offsets'),
    EV_Reflective_Band_5                 float8  OPTIONS (cat_name 'mod021km', dataset 'EV_500_Aggr1km_RefSB', prefix '2', scale 'reflectance_scales', offset 'reflectance_offsets'),
    EV_Reflective_Band_6                 float8  OPTIONS (cat_name 'mod021km', dataset 'EV_500_Aggr1km_RefSB', prefix '3', scale 'reflectance_scales', offset 'reflectance_offsets'),
    EV_Reflective_Band_7                 float8  OPTIONS (cat_name 'mod021km', dataset 'EV_500_Aggr1km_RefSB', prefix '4', scale 'reflectance_scales', offset 'reflectance_offsets'),
    EV_Reflective_Band_8                 float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB', prefix '0',  scale 'reflectance_scales', offset 'reflectance_offsets'),
    EV_Reflective_Band_9                 float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB', prefix '1',  scale 'reflectance_scales', offset 'reflectance_offsets'),
    EV_Reflective_Band_10                float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB', prefix '2',  scale 'reflectance_scales', offset 'reflectance_offsets'),
    EV_Reflective_Band_11                float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB', prefix '3',  scale 'reflectance_scales', offset 'reflectance_offsets'),
    EV_Reflective_Band_12                float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB', prefix '4',  scale 'reflectance_scales', offset 'reflectance_offsets'),
    EV_Reflective_Band_13lo              float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB', prefix '5',  scale 'reflectance_scales', offset 'reflectance_offsets'),
    EV_Reflective_Band_13hi              float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB', prefix '6',  scale 'reflectance_scales', offset 'reflectance_offsets'),
    EV_Reflective_Band_14lo              float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB', prefix '7',  scale 'reflectance_scales', offset 'reflectance_offsets'),
    EV_Reflective_Band_14hi              float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB', prefix '8',  scale 'reflectance_scales', offset 'reflectance_offsets'),
    EV_Reflective_Band_15                float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB', prefix '9',  scale 'reflectance_scales', offset 'reflectance_offsets'),
    EV_Reflective_Band_16                float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB', prefix '10', scale 'reflectance_scales', offset 'reflectance_offsets'),
    EV_Reflective_Band_17                float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB', prefix '11', scale 'reflectance_scales', offset 'reflectance_offsets'),
    EV_Reflective_Band_18                float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB', prefix '12', scale 'reflectance_scales', offset 'reflectance_offsets'),
    EV_Reflective_Band_19                float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB', prefix '13', scale 'reflectance_scales', offset 'reflectance_offsets'),
    -- EV_Reflective_Band_26Day             float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB', prefix '14'),
    EV_Emissive_Band_20                  float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive', prefix '0',  scale 'radiance_scales', offset 'radiance_offsets'),
    EV_Emissive_Band_21                  float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive', prefix '1',  scale 'radiance_scales', offset 'radiance_offsets'),
    EV_Emissive_Band_22                  float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive', prefix '2',  scale 'radiance_scales', offset 'radiance_offsets'),
    EV_Emissive_Band_23                  float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive', prefix '3',  scale 'radiance_scales', offset 'radiance_offsets'),
    EV_Emissive_Band_24                  float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive', prefix '4',  scale 'radiance_scales', offset 'radiance_offsets'),
    EV_Emissive_Band_25                  float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive', prefix '5',  scale 'radiance_scales', offset 'radiance_offsets'),
    EV_Reflective_Band_26                float8  OPTIONS (cat_name 'mod021km', dataset 'EV_Band26', scale 'reflectance_scales', offset 'reflectance_offsets'),
    EV_Emissive_Band_27                  float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive', prefix '6',  scale 'radiance_scales', offset 'radiance_offsets'),
    EV_Emissive_Band_28                  float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive', prefix '7',  scale 'radiance_scales', offset 'radiance_offsets'),
    EV_Emissive_Band_29                  float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive', prefix '8',  scale 'radiance_scales', offset 'radiance_offsets'),
    EV_Emissive_Band_30                  float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive', prefix '9',  scale 'radiance_scales', offset 'radiance_offsets'),
    EV_Emissive_Band_31                  float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive', prefix '10', scale 'radiance_scales', offset 'radiance_offsets'),
    EV_Emissive_Band_32                  float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive', prefix '11', scale 'radiance_scales', offset 'radiance_offsets'),
    EV_Emissive_Band_33                  float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive', prefix '12', scale 'radiance_scales', offset 'radiance_offsets'),
    EV_Emissive_Band_34                  float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive', prefix '13', scale 'radiance_scales', offset 'radiance_offsets'),
    EV_Emissive_Band_35                  float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive', prefix '14', scale 'radiance_scales', offset 'radiance_offsets'),
    EV_Emissive_Band_36                  float8  OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive', prefix '15', scale 'radiance_scales', offset 'radiance_offsets'),

    EV_Reflective_Band_1_UncertIndex     int2    OPTIONS (cat_name 'mod021km', dataset 'EV_250_Aggr1km_RefSB_Uncert_Indexes', prefix '0'),
    EV_Reflective_Band_2_UncertIndex     int2    OPTIONS (cat_name 'mod021km', dataset 'EV_250_Aggr1km_RefSB_Uncert_Indexes', prefix '1'),
    EV_Reflective_Band_3_UncertIndex     int2    OPTIONS (cat_name 'mod021km', dataset 'EV_500_Aggr1km_RefSB_Uncert_Indexes', prefix '0'),
    EV_Reflective_Band_4_UncertIndex     int2    OPTIONS (cat_name 'mod021km', dataset 'EV_500_Aggr1km_RefSB_Uncert_Indexes', prefix '1'),
    EV_Reflective_Band_5_UncertIndex     int2    OPTIONS (cat_name 'mod021km', dataset 'EV_500_Aggr1km_RefSB_Uncert_Indexes', prefix '2'),
    EV_Reflective_Band_6_UncertIndex     int2    OPTIONS (cat_name 'mod021km', dataset 'EV_500_Aggr1km_RefSB_Uncert_Indexes', prefix '3'),
    EV_Reflective_Band_7_UncertIndex     int2    OPTIONS (cat_name 'mod021km', dataset 'EV_500_Aggr1km_RefSB_Uncert_Indexes', prefix '4'),
    EV_Reflective_Band_8_UncertIndex     int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB_Uncert_Indexes', prefix '0'),
    EV_Reflective_Band_9_UncertIndex     int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB_Uncert_Indexes', prefix '1'),
    EV_Reflective_Band_10_UncertIndex    int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB_Uncert_Indexes', prefix '2'),
    EV_Reflective_Band_11_UncertIndex    int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB_Uncert_Indexes', prefix '3'),
    EV_Reflective_Band_12_UncertIndex    int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB_Uncert_Indexes', prefix '4'),
    EV_Reflective_Band_13lo_UncertIndex  int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB_Uncert_Indexes', prefix '5'),
    EV_Reflective_Band_13hi_UncertIndex  int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB_Uncert_Indexes', prefix '6'),
    EV_Reflective_Band_14lo_UncertIndex  int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB_Uncert_Indexes', prefix '7'),
    EV_Reflective_Band_14hi_UncertIndex  int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB_Uncert_Indexes', prefix '8'),
    EV_Reflective_Band_15_UncertIndex    int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB_Uncert_Indexes', prefix '9'),
    EV_Reflective_Band_16_UncertIndex    int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB_Uncert_Indexes', prefix '10'),
    EV_Reflective_Band_17_UncertIndex    int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB_Uncert_Indexes', prefix '11'),
    EV_Reflective_Band_18_UncertIndex    int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB_Uncert_Indexes', prefix '12'),
    EV_Reflective_Band_19_UncertIndex    int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB_Uncert_Indexes', prefix '13'),
    -- EV_Reflective_Band_26Day_UncertIndex int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_RefSB_Uncert_Indexes', prefix '14'),
    EV_Emissive_Band_20_UncertIndex      int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive_Uncert_Indexes', prefix '0'),
    EV_Emissive_Band_21_UncertIndex      int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive_Uncert_Indexes', prefix '1'),
    EV_Emissive_Band_22_UncertIndex      int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive_Uncert_Indexes', prefix '2'),
    EV_Emissive_Band_23_UncertIndex      int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive_Uncert_Indexes', prefix '3'),
    EV_Emissive_Band_24_UncertIndex      int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive_Uncert_Indexes', prefix '4'),
    EV_Emissive_Band_25_UncertIndex      int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive_Uncert_Indexes', prefix '5'),
    EV_Reflective_Band_26_UncertIndex    int2    OPTIONS (cat_name 'mod021km', dataset 'EV_Band26_Uncert_Indexes'),
    EV_Emissive_Band_27_UncertIndex      int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive_Uncert_Indexes', prefix '6'),
    EV_Emissive_Band_28_UncertIndex      int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive_Uncert_Indexes', prefix '7'),
    EV_Emissive_Band_29_UncertIndex      int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive_Uncert_Indexes', prefix '8'),
    EV_Emissive_Band_30_UncertIndex      int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive_Uncert_Indexes', prefix '9'),
    EV_Emissive_Band_31_UncertIndex      int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive_Uncert_Indexes', prefix '10'),
    EV_Emissive_Band_32_UncertIndex      int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive_Uncert_Indexes', prefix '11'),
    EV_Emissive_Band_33_UncertIndex      int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive_Uncert_Indexes', prefix '12'),
    EV_Emissive_Band_34_UncertIndex      int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive_Uncert_Indexes', prefix '13'),
    EV_Emissive_Band_35_UncertIndex      int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive_Uncert_Indexes', prefix '14'),
    EV_Emissive_Band_36_UncertIndex      int2    OPTIONS (cat_name 'mod021km', dataset 'EV_1KM_Emissive_Uncert_Indexes', prefix '15'),

    EV_Reflective_Band_1_Samples_Used    int2    OPTIONS (cat_name 'mod021km', dataset 'EV_250_Aggr1km_RefSB_Samples_Used', prefix '0'),
    EV_Reflective_Band_2_Samples_Used    int2    OPTIONS (cat_name 'mod021km', dataset 'EV_250_Aggr1km_RefSB_Samples_Used', prefix '1'),
    EV_Reflective_Band_3_Samples_Used    int2    OPTIONS (cat_name 'mod021km', dataset 'EV_500_Aggr1km_RefSB_Samples_Used', prefix '0'),
    EV_Reflective_Band_4_Samples_Used    int2    OPTIONS (cat_name 'mod021km', dataset 'EV_500_Aggr1km_RefSB_Samples_Used', prefix '1'),
    EV_Reflective_Band_5_Samples_Used    int2    OPTIONS (cat_name 'mod021km', dataset 'EV_500_Aggr1km_RefSB_Samples_Used', prefix '2'),
    EV_Reflective_Band_6_Samples_Used    int2    OPTIONS (cat_name 'mod021km', dataset 'EV_500_Aggr1km_RefSB_Samples_Used', prefix '3'),
    EV_Reflective_Band_7_Samples_Used    int2    OPTIONS (cat_name 'mod021km', dataset 'EV_500_Aggr1km_RefSB_Samples_Used', prefix '4'),

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
    Surface_Reflectance_Band_1        float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 1',  inverse_scale 'true'),
    Surface_Reflectance_Band_2        float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 2',  inverse_scale 'true'),
    Surface_Reflectance_Band_3        float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 3',  inverse_scale 'true'),
    Surface_Reflectance_Band_4        float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 4',  inverse_scale 'true'),
    Surface_Reflectance_Band_5        float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 5',  inverse_scale 'true'),
    Surface_Reflectance_Band_6        float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 6',  inverse_scale 'true'),
    Surface_Reflectance_Band_7        float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 7',  inverse_scale 'true'),
    Surface_Reflectance_Band_8        float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 8',  inverse_scale 'true'),
    Surface_Reflectance_Band_9        float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 9',  inverse_scale 'true'),
    Surface_Reflectance_Band_10       float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 10', inverse_scale 'true'),
    Surface_Reflectance_Band_11       float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 11', inverse_scale 'true'),
    Surface_Reflectance_Band_12       float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 12', inverse_scale 'true'),
    Surface_Reflectance_Band_13       float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 13', inverse_scale 'true'),
    Surface_Reflectance_Band_14       float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 14', inverse_scale 'true'),
    Surface_Reflectance_Band_15       float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 15', inverse_scale 'true'),
    Surface_Reflectance_Band_16       float8  OPTIONS (cat_name 'mod09', dataset '1km Surface Reflectance Band 16', inverse_scale 'true'),

    Reflectance_Band_Quality          int4    OPTIONS (cat_name 'mod09', dataset '1km Reflectance Band Quality'),
    Reflectance_Data_State_QA         int2    OPTIONS (cat_name 'mod09', dataset '1km Reflectance Data State QA'),

    Atmospheric_Optical_Depth_Band_1  float8  OPTIONS (cat_name 'mod09', dataset '1km Atmospheric Optical Depth Band 1', inverse_scale 'true'),
    Atmospheric_Optical_Depth_Band_3  float8  OPTIONS (cat_name 'mod09', dataset '1km Atmospheric Optical Depth Band 3', inverse_scale 'true'),
    Atmospheric_Optical_Depth_Band_8  float8  OPTIONS (cat_name 'mod09', dataset '1km Atmospheric Optical Depth Band 8', inverse_scale 'true'),
    
    Atmospheric_Optical_Depth_Model   int2    OPTIONS (cat_name 'mod09', dataset '1km Atmospheric Optical Depth Model'),
    Atmospheric_Optical_Depth_Band_QA int2    OPTIONS (cat_name 'mod09', dataset '1km Atmospheric Optical Depth Band QA'),
    Atmospheric_Optical_Depth_Band_CM int2    OPTIONS (cat_name 'mod09', dataset '1km Atmospheric Optical Depth Band CM'),

    Band_3_Path_Radiance              float8  OPTIONS (cat_name 'mod09', dataset '1km Band 3 Path Radiance', inverse_scale 'true'),

-- MOD35 layers
    Cloud_Mask                        bit(48) OPTIONS (cat_name 'mod35_l2', bitmap_type 'prefix',  bitmap_dims '1', dataset 'Cloud_Mask'),
    Cloud_Mask_QA                     bit(80) OPTIONS (cat_name 'mod35_l2', bitmap_type 'postfix', bitmap_dims '1', dataset 'Quality_Assurance'),

-- MOD04 layers
-- 10km layers are commented out
    -- Aerosol_Type_Land                             int2    OPTIONS (cat_name 'mod04_l2', factor '10', dataset 'Aerosol_Type_Land'),
    -- Angstrom_Exponent_Land                        float8  OPTIONS (cat_name 'mod04_l2', factor '10', dataset 'Angstrom_Exponent_Land'),
    -- Cloud_Fraction_Land                           float8  OPTIONS (cat_name 'mod04_l2', factor '10', dataset 'Cloud_Fraction_Land'),
    -- Cloud_Fraction_Ocean                          float8  OPTIONS (cat_name 'mod04_l2', factor '10', dataset 'Cloud_Fraction_Ocean'),
    -- Corrected_Optical_Depth_Land_wav2p1           float8  OPTIONS (cat_name 'mod04_l2', factor '10', dataset 'Corrected_Optical_Depth_Land_wav2p1'),
    -- Deep_Blue_Aerosol_Optical_Depth_550_Land      float8  OPTIONS (cat_name 'mod04_l2', factor '10', dataset 'Deep_Blue_Aerosol_Optical_Depth_550_Land'),
    -- Deep_Blue_Aerosol_Optical_Depth_550_Land_STD  float8  OPTIONS (cat_name 'mod04_l2', factor '10', dataset 'Deep_Blue_Aerosol_Optical_Depth_550_Land_STD'),
    -- Deep_Blue_Angstrom_Exponent_Land              float8  OPTIONS (cat_name 'mod04_l2', factor '10', dataset 'Deep_Blue_Angstrom_Exponent_Land'),
    -- Fitting_Error_Land                            float8  OPTIONS (cat_name 'mod04_l2', factor '10', dataset 'Fitting_Error_Land'),
    -- Image_Optical_Depth_Land_And_Ocean            float8  OPTIONS (cat_name 'mod04_l2', factor '10', dataset 'Image_Optical_Depth_Land_And_Ocean'),
    -- Mass_Concentration_Land                       float8  OPTIONS (cat_name 'mod04_l2', factor '10', dataset 'Mass_Concentration_Land'),
    -- Number_Pixels_Used_Ocean                      int2    OPTIONS (cat_name 'mod04_l2', factor '10', dataset 'Number_Pixels_Used_Ocean'),
    -- Optical_Depth_Land_And_Ocean                  float8  OPTIONS (cat_name 'mod04_l2', factor '10', dataset 'Optical_Depth_Land_And_Ocean'),
    -- Optical_Depth_Ratio_Small_Land                float8  OPTIONS (cat_name 'mod04_l2', factor '10', dataset 'Optical_Depth_Ratio_Small_Land'),
    -- Optical_Depth_Ratio_Small_Land_And_Ocean      float8  OPTIONS (cat_name 'mod04_l2', factor '10', dataset 'Optical_Depth_Ratio_Small_Land_And_Ocean'),
    -- Scattering_Angle                              float8  OPTIONS (cat_name 'mod04_l2', factor '10', dataset 'Scattering_Angle'),
    -- MOD04_Quality_Assurance_Ocean                 bit(40) OPTIONS (cat_name 'mod04_l2', factor '10', bitmap_type 'postfix', bitmap_dims '1', dataset 'Quality_Assurance_Ocean'),
    -- MOD04_Quality_Assurance_Land                  bit(40) OPTIONS (cat_name 'mod04_l2', factor '10', bitmap_type 'postfix', bitmap_dims '1', dataset 'Quality_Assurance_Land'),
    -- MOD04_Quality_Assurance_Crit_Ref_Land         bit(40) OPTIONS (cat_name 'mod04_l2', factor '10', bitmap_type 'postfix', bitmap_dims '1', dataset 'Quality_Assurance_Crit_Ref_Land'),
    -- TODO: lot more

-- MOD05 layers
    Water_Vapor_Near_Infrared                     float8  OPTIONS (cat_name 'mod05_l2', dataset 'Water_Vapor_Near_Infrared'),
    Water_Vapor_Correction_Factors                int2    OPTIONS (cat_name 'mod05_l2', dataset 'Water_Vapor_Correction_Factors'),
    MOD05_Quality_Assurance_Near_Infrared         bit(8)  OPTIONS (cat_name 'mod05_l2', bitmap_type 'postfix', bitmap_dims '1', dataset 'Quality_Assurance_Near_Infrared')

    -- Water_Vapor_Infrared                          float8  OPTIONS (cat_name 'mod05_l2', factor '5', dataset 'Water_Vapor_Infrared'),
    -- MOD05_Quality_Assurance_Infrared              bit(40) OPTIONS (cat_name 'mod05_l2', factor '5', bitmap_type 'postfix', bitmap_dims '1', dataset 'Quality_Assurance_Infrared'),

-- MOD07 layers
    -- K_Index                     float8  OPTIONS (cat_name 'mod07_l2', factor '5', dataset 'K_Index'),
    -- Lifted_Index                float8  OPTIONS (cat_name 'mod07_l2', factor '5', dataset 'Lifted_Index'),
    -- Surface_Elevation           float8  OPTIONS (cat_name 'mod07_l2', factor '5', dataset 'Surface_Elevation'),
    -- Surface_Temperature         float8  OPTIONS (cat_name 'mod07_l2', factor '5', dataset 'Surface_Temperature'),
    -- Total_Ozone                 float8  OPTIONS (cat_name 'mod07_l2', factor '5', dataset 'Total_Ozone'),
    -- Total_Totals                float8  OPTIONS (cat_name 'mod07_l2', factor '5', dataset 'Total_Totals'),
    -- Tropopause_Height           float8  OPTIONS (cat_name 'mod07_l2', factor '5', dataset 'Tropopause_Height'),
    -- Water_Vapor                 float8  OPTIONS (cat_name 'mod07_l2', factor '5', dataset 'Water_Vapor'),
    -- Water_Vapor_Direct          float8  OPTIONS (cat_name 'mod07_l2', factor '5', dataset 'Water_Vapor_Direct'),
    -- Water_Vapor_High            float8  OPTIONS (cat_name 'mod07_l2', factor '5', dataset 'Water_Vapor_High'),
    -- Water_Vapor_Low             float8  OPTIONS (cat_name 'mod07_l2', factor '5', dataset 'Water_Vapor_Low'),
    -- Surface_Pressure            float8  OPTIONS (cat_name 'mod07_l2', factor '5', dataset 'Surface_Pressure'),

    -- MOD07_Processing_Flag             int2    OPTIONS (cat_name 'mod07_l2', factor '5', dataset 'Processing_Flag'),
    -- MOD07_Quality_Assurance           bit(80) OPTIONS (cat_name 'mod07_l2', factor '5', bitmap_type 'postfix', bitmap_dims '1', dataset 'Quality_Assurance'),
    -- MOD07_Quality_Assurance_Infrared  bit(40) OPTIONS (cat_name 'mod07_l2', factor '5', bitmap_type 'postfix', bitmap_dims '1', dataset 'Quality_Assurance_Infrared')
    --TODO:
    -- Retrieved_Height_Profile (20, 306, 270)
    -- Guess_Temperature_Profile (20, 306, 270)
    -- Brightness_Temperature (12, 306, 270)
    -- Guess_Moisture_Profile (20, 306, 270)
    -- Retrieved_Moisture_Profile (20, 306, 270)
    -- Retrieved_Temperature_Profile (20, 306, 270)

) SERVER hvault_service
  OPTIONS (catalog 'catalog',
           driver 'modis_swath',
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
    Snow_Cover                  int2    OPTIONS (cat_name 'mod10_l2', dataset 'Snow_Cover'),
    Fractional_Snow_Cover       int2    OPTIONS (cat_name 'mod10_l2', dataset 'Fractional_Snow_Cover'),
    Snow_Cover_Pixel_QA         int2    OPTIONS (cat_name 'mod10_l2', dataset 'Snow_Cover_Pixel_QA'),

-- MOD09 layers
    Surface_Reflectance_Band_1  float8  OPTIONS (cat_name 'mod09', dataset '500m Surface Reflectance Band 1'),
    Surface_Reflectance_Band_2  float8  OPTIONS (cat_name 'mod09', dataset '500m Surface Reflectance Band 2'),
    Surface_Reflectance_Band_3  float8  OPTIONS (cat_name 'mod09', dataset '500m Surface Reflectance Band 3'),
    Surface_Reflectance_Band_4  float8  OPTIONS (cat_name 'mod09', dataset '500m Surface Reflectance Band 4'),
    Surface_Reflectance_Band_5  float8  OPTIONS (cat_name 'mod09', dataset '500m Surface Reflectance Band 5'),
    Surface_Reflectance_Band_6  float8  OPTIONS (cat_name 'mod09', dataset '500m Surface Reflectance Band 6'),
    Surface_Reflectance_Band_7  float8  OPTIONS (cat_name 'mod09', dataset '500m Surface Reflectance Band 7'),
    Reflectance_Band_Quality    int4    OPTIONS (cat_name 'mod09', dataset '500m Reflectance Band Quality')

) SERVER hvault_service
  OPTIONS (catalog 'catalog',
           driver 'modis_swath',
           shift_longitude 'true');
