SELECT hvault_create_catalog('mod09_catalog');
select hvault_mass_load_modis_swath(
    'mod09_catalog', '/mnt/ifs-gis/ftp/terra/modis/archive', 'MOD09.*.hdf');
select hvault_mass_load_modis_swath(
    'mod09_catalog', '/mnt/ifs-gis/ftp/aqua/modis/archive', 'MYD09.*.hdf');


SELECT hvault_create_catalog('mod03_catalog');
select hvault_mass_load_modis_swath(
    'mod03_catalog', '/mnt/ifs-gis/ftp/terra/modis/archive', 'MOD03.*.hdf');
select hvault_mass_load_modis_swath(
    'mod03_catalog', '/mnt/ifs-gis/ftp/aqua/modis/archive', 'MYD03.*.hdf');

SELECT hvault_create_catalog('mod04_catalog');
