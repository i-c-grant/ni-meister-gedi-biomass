BEGIN;
-- Add a new column to the menlove_healey_biohex table
ALTER TABLE menlove_healey_biohex
ADD COLUMN biwf_temp_conifer_na_adj NUMERIC,
ADD COLUMN biwf_temp_broadleaf_na_adj NUMERIC,
ADD COLUMN biwf_temp_conifer_all_adj NUMERIC,
ADD COLUMN biwf_temp_broadleaf_all_adj NUMERIC;

-- Update the new columns with the values from the original columns
UPDATE menlove_healey_biohex
SET biwf_temp_conifer_na_adj = biwf_temp_conifer_na * 2.56,
biwf_temp_broadleaf_na_adj = biwf_temp_broadleaf_na * 0.58,
biwf_temp_conifer_all_adj = biwf_temp_conifer_all * 1.47,
biwf_temp_broadleaf_all_adj = biwf_temp_broadleaf_all * 1;

COMMIT;


