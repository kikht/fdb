select * from test_catalog limit 10;

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


SELECT DISTINCT file_id FROM test_catalog 
    WHERE Surface_Reflectance_Band_1 IS NOT NULL;

SELECT DISTINCT file_id FROM test_catalog 
    WHERE Surface_Reflectance_Band_1 IS NULL;

SELECT COUNT(*), file_id FROM test_catalog 
    WHERE Surface_Reflectance_Band_1 IS NULL GROUP BY file_id;

SELECT MAX(line_id), MAX(Surface_Reflectance_Band_1) 
    FROM test_catalog GROUP BY file_id;


SELECT file_id, line_id, sample_id, 
       Surface_Reflectance_Band_1, 
       Surface_Reflectance_Band_3, 
       Surface_Reflectance_Band_4, 
       ST_AsText(point) point 
    FROM test_catalog 
    WHERE ST_Contains(footprint, ST_GeometryFromText('POINT(64.90 43.19)'));

SELECT file_id, line_id, sample_id, 
       Surface_Reflectance_Band_1, 
       Surface_Reflectance_Band_3, 
       Surface_Reflectance_Band_4, 
       ST_AsText(point) point 
    FROM test_catalog 
    WHERE ST_Contains(footprint, ST_GeometryFromText('POINT(65.00 43.19)'));

SELECT file_id, line_id, sample_id, 
       Surface_Reflectance_Band_1, 
       Surface_Reflectance_Band_3, 
       Surface_Reflectance_Band_4, 
       ST_AsText(point) point 
    FROM test_catalog 
    WHERE ST_Contains(footprint, ST_GeometryFromText('POINT(100.90 61.16)'));

SELECT file_id, line_id, sample_id, 
       Surface_Reflectance_Band_1, 
       Surface_Reflectance_Band_3, 
       Surface_Reflectance_Band_4, 
       ST_AsText(point) point 
    FROM test_catalog 
    WHERE ST_Contains(footprint, ST_GeometryFromText('POINT(65.90 61.16)'));

SELECT file_id, line_id, sample_id, 
       Surface_Reflectance_Band_1, 
       Surface_Reflectance_Band_3, 
       Surface_Reflectance_Band_4, 
       ST_AsText(point) point 
    FROM test_catalog 
    WHERE ST_Contains(footprint, ST_GeometryFromText('POINT(69.90 61.16)'));

SELECT file_id, line_id, sample_id, 
       Surface_Reflectance_Band_1, 
       Surface_Reflectance_Band_3, 
       Surface_Reflectance_Band_4, 
       ST_AsText(point) point 
    FROM test_catalog 
    WHERE ST_Contains(footprint, ST_GeometryFromText('POINT(100.90 61.16)')) 
    ORDER BY time;


SELECT file_id, line_id, sample_id 
    FROM test_catalog 
    WHERE ST_Within(point, ST_GeometryFromText('POLYGON((65.89 61.16,65.92 61.16,65.92 61.15,65.89 61.15,65.89 61.16))')) 
    LIMIT 10;
SELECT file_id, line_id, sample_id 
    FROM test_catalog 
    WHERE ST_Contains(point, ST_GeometryFromText('POLYGON((66.89 61.16,66.92 61.16,66.92 61.15,66.89 61.15,66.89 61.16))')) 
    LIMIT 10;
