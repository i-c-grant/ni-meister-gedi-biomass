-- Calculate the count of fifth_run_results points within
-- each menlove_healey_biohex polygon and store the results

BEGIN;
CREATE TEMPORARY TABLE temp_results AS
SELECT 
    mhb.ushexes_id AS hex_id,
    COUNT(fr.biwf) AS count_biwf_fifth_run,
    COUNT(fr.l4_agbd) AS count_l4_agbd

