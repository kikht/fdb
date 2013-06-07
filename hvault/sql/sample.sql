select avg(((Reflectance_band_quality & x'3ffffffc'::int) = 0)::int), file_id 
    from test_catalog group by file_id;


SELECT d.file_id, d.line_id, d.sample_id, ST_AsText(point) 
    FROM test_catalog d 
    JOIN hdf_catalog f ON d.file_id = f.file_id 
    WHERE NOT ST_Contains(f.footprint, d.point);

SELECT d.file_id, d.line_id, d.sample_id, ST_AsText(point) 
    FROM test_catalog d 
    JOIN hdf_catalog f ON d.file_id = f.file_id 
    WHERE NOT f.footprint && d.point;

SELECT * FROM test_catalog WHERE NOT ST_Contains(footprint, point);


SELECT DISTINCT file_id FROM test_catalog;

SELECT DISTINCT file_id FROM test_catalog 
    WHERE Surface_Reflectance_Band_1 IS NULL;

SELECT COUNT(*), file_id FROM test_catalog 
    WHERE Surface_Reflectance_Band_1 IS NULL GROUP BY file_id;

SELECT MAX(line_id) FROM test_catalog GROUP BY file_id;


SELECT file_id, line_id, sample_id, 
       Surface_Reflectance_Band_1, 
       Surface_Reflectance_Band_3, 
       Surface_Reflectance_Band_4, 
       ST_AsText(point) point 
    FROM test_catalog 
    WHERE ST_Contains(footprint, ST_GeometryFromText('POINT( 43.19 64.90)'));

SELECT file_id, line_id, sample_id, 
       Surface_Reflectance_Band_1, 
       Surface_Reflectance_Band_3, 
       Surface_Reflectance_Band_4, 
       ST_AsText(point) point 
    FROM test_catalog 
    WHERE ST_Contains(footprint, ST_GeometryFromText('POINT( 43.19 65.00)'));

SELECT file_id, line_id, sample_id, 
       Surface_Reflectance_Band_1, 
       Surface_Reflectance_Band_3, 
       Surface_Reflectance_Band_4, 
       ST_AsText(point) point 
    FROM test_catalog 
    WHERE ST_Contains(footprint, ST_GeometryFromText('POINT( 61.16 100.90)'));

SELECT file_id, line_id, sample_id, 
       Surface_Reflectance_Band_1, 
       Surface_Reflectance_Band_3, 
       Surface_Reflectance_Band_4, 
       ST_AsText(point) point 
    FROM test_catalog 
    WHERE ST_Contains(footprint, ST_GeometryFromText('POINT( 61.16 65.90)'));

SELECT file_id, line_id, sample_id, 
       Surface_Reflectance_Band_1, 
       Surface_Reflectance_Band_3, 
       Surface_Reflectance_Band_4, 
       ST_AsText(point) point 
    FROM test_catalog 
    WHERE ST_Contains(footprint, ST_GeometryFromText('POINT( 61.16 69.90)'));

SELECT file_id, line_id, sample_id, 
       Surface_Reflectance_Band_1, 
       Surface_Reflectance_Band_3, 
       Surface_Reflectance_Band_4, 
       ST_AsText(point) point 
    FROM test_catalog 
    WHERE ST_Contains(footprint, ST_GeometryFromText('POINT( 61.16 100.90)')) 
    ORDER BY time;


SELECT file_id, line_id, sample_id 
    FROM test_catalog 
    WHERE ST_Within(point, ST_GeometryFromText('POLYGON((61.16 65.89,61.16 65.92,61.15 65.92,61.15 65.89,61.16 65.89))')) 
    LIMIT 10;
SELECT file_id, line_id, sample_id 
    FROM test_catalog 
    WHERE ST_Contains(point, ST_GeometryFromText('POLYGON((61.16 66.89,61.16 66.92,61.15 66.92,61.15 66.89,61.16 66.89))')) 
    LIMIT 10;
