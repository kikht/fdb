SELECT d.file_id, d.line_id, d.sample_id, ST_AsText(pnt) 
    FROM test_catalog d 
    JOIN hdf_catalog f ON d.file_id = f.file_id 
    WHERE NOT ST_Contains(f.footprint, d.pnt);

SELECT * FROM test_catalog WHERE NOT ST_Contains(footprint, pnt);


SELECT DISTINCT file_id FROM test_catalog;

SELECT DISTINCT file_id FROM test_catalog WHERE band1 IS NULL;

SELECT COUNT(*), file_id FROM test_catalog WHERE band1 IS NULL GROUP BY file_id;

SELECT MAX(line_id) pnt FROM test_catalog GROUP BY file_id;


SELECT file_id, line_id, sample_id, band1, band3, band4, ST_AsText(pnt) pnt 
    FROM test_catalog 
    WHERE ST_Contains(footpt, ST_GeometryFromText('POINT( 43.19 64.90)'));

SELECT file_id, line_id, sample_id, band1, band3, band4, ST_AsText(pnt) pnt 
    FROM test_catalog 
    WHERE ST_Contains(footpt, ST_GeometryFromText('POINT( 43.19 65.00)'));

SELECT file_id, line_id, sample_id, band1, band3, band4, ST_AsText(pnt) pnt 
    FROM test_catalog 
    WHERE ST_Contains(footpt, ST_GeometryFromText('POINT( 61.16 100.90)'));

SELECT file_id, line_id, sample_id, band1, band3, band4, ST_AsText(pnt) pnt 
    FROM test_catalog 
    WHERE ST_Contains(footpt, ST_GeometryFromText('POINT( 61.16 65.90)'));

SELECT file_id, line_id, sample_id, band1, band3, band4, ST_AsText(pnt) pnt 
    FROM test_catalog 
    WHERE ST_Contains(footpt, ST_GeometryFromText('POINT( 61.16 69.90)'));

SELECT file_id, line_id, sample_id, band1, band3, band4, ST_AsText(pnt) pnt 
    FROM test_catalog 
    WHERE ST_Contains(footpt, ST_GeometryFromText('POINT( 61.16 100.90)')) 
    ORDER BY tstamp;


SELECT file_id, line_id, sample_id 
    FROM test_catalog 
    WHERE ST_Contains(ST_GeometryFromText('POLYGON((61.16 65.89,61.16 65.92,61.15 65.92,61.15 65.89,61.16 65.89))'), pnt) 
    LIMIT 10;
SELECT file_id, line_id, sample_id 
    FROM test_catalog 
    WHERE ST_Contains(ST_GeometryFromText('POLYGON((61.16 66.89,61.16 66.92,61.15 66.92,61.15 66.89,61.16 66.89))'), pnt) 
    LIMIT 10;
