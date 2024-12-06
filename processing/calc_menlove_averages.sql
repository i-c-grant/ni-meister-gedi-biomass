-- Set the number of parallel workers (adjust as needed)
SET max_parallel_workers_per_gather = 16;
SET max_parallel_workers = 16;

-- Create a temporary table to store the results
BEGIN;
CREATE TEMPORARY TABLE temp_results AS
SELECT 
    mhb.ushexes_id AS hex_id,
    AVG(results.biwf) AS avg_biwf
FROM 
    menlove_healey_biohex mhb
JOIN 
    conus_raster_params results
ON 
    ST_Contains(mhb.geom, results.geom)
GROUP BY 
    mhb.ushexes_id;

COMMIT;

BEGIN;
-- Add a new column to the menlove_healey_biohex table
ALTER TABLE menlove_healey_biohex
ADD COLUMN biwf_conus_raster_params NUMERIC;

-- Update the new column with the calculated average values
UPDATE menlove_healey_biohex mhb
SET 
    biwf_conus_raster_params = tr.avg_biwf
FROM temp_results tr
WHERE mhb.ushexes_id = tr.hex_id;

-- Drop the temporary table
DROP TABLE temp_results;

COMMIT;
