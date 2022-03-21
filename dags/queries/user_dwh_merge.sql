set time zone UTC;
INSERT INTO twitter.user
(    
    created_at,
    id,
    name,
    username,
    processed_at
) SELECT
    to_timestamp(created_at, 'YYYY-MM-DD HH24:mi:ss')::timestamp created_at,
    id,
    name,    
    username,
    processed_at
FROM (
    SELECT *,
    row_number() over(partition by id) as rn
    FROM twitter_staging.user SUi
    WHERE processed_at = '{{ ts_nodash }}'
    AND NOT EXISTS(SELECT 1 FROM twitter.user TU WHERE TU.id = SUi.id AND TU.processed_at > SUi.processed_at)
) SU WHERE rn = 1
ON CONFLICT (id) DO UPDATE
SET    
    username=EXCLUDED.username,
    created_at=EXCLUDED.created_at,
    name=EXCLUDED.name,
    processed_at=EXCLUDED.processed_at,
    updated_at=NOW();

DELETE FROM twitter_staging.user
WHERE processed_at = '{{ ts_nodash }}';