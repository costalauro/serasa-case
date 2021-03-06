set time zone UTC;
INSERT INTO twitter.tweet
(
    id,
    text,
    created_at,
    author_id,
    conversation_id,
    in_reply_to_user_id,
    like_count,
    quote_count,
    reply_count,
    processed_at
) SELECT 
    id,
    text,    
    to_timestamp(created_at, 'YYYY-MM-DD HH24:mi:ss')::timestamp created_at,
    author_id,
    conversation_id,
    in_reply_to_user_id,
    like_count,
    quote_count,
    reply_count,        
    processed_at
FROM (
    SELECT *,
    row_number() over(partition by id) as rn
    FROM twitter_staging.tweet STi
    WHERE processed_at = '{{ ts_nodash }}'
    AND NOT EXISTS(SELECT 1 FROM twitter.tweet TU WHERE TU.id = STi.id AND TU.processed_at > STi.processed_at)
) ST WHERE rn = 1
ON CONFLICT (id) DO UPDATE
SET
    id=EXCLUDED.id,
    text=EXCLUDED.text,
    created_at=EXCLUDED.created_at,
    author_id=EXCLUDED.author_id,
    conversation_id=EXCLUDED.conversation_id,
    in_reply_to_user_id=EXCLUDED.in_reply_to_user_id,
    like_count=EXCLUDED.like_count,
    quote_count=EXCLUDED.quote_count,
    reply_count=EXCLUDED.reply_count,
    processed_at=EXCLUDED.processed_at,
    updated_at=NOW();

DELETE FROM twitter_staging.tweet
WHERE processed_at = '{{ ts_nodash }}';