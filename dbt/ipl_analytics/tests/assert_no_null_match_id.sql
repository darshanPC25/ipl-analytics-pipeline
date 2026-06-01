SELECT *
FROM {{ ref('stg_ipl_matches') }}
WHERE match_id IS NULL