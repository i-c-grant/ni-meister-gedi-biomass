-- Set the number of parallel workers (adjust as needed)
SET max_parallel_workers_per_gather = 16;
SET max_parallel_workers = 16;

-- Create a temporary table to store the results
BEGIN;
CREATE TEMPORARY TABLE temp_results AS
SELECT 
    conus_cells.fid AS cell_id,
    AVG(fr.biwf) AS avg_biwf
FROM 
    conus_cells
    JOIN fourth_run_results fr ON ST_Contains(conus_cells.geom, fr.geom)
GROUP BY 
    conus_cells.fid;
COMMIT;

BEGIN;
-- Add a new column to the conus_cells table
ALTER TABLE conus_cells ADD COLUMN biwf_fourth_run NUMERIC;

-- Update the new column with the calculated average values
UPDATE conus_cells c
SET biwf_fourth_run = tr.avg_biwf
FROM temp_results tr
WHERE c.fid = tr.cell_id;

-- Drop the temporary table
DROP TABLE temp_results;
COMMIT;
