SELECT "objectId" AS story_id, "title", "author", "contentType", "lang", "releasedAt"::date as publication_date, 
    "recommendationIndex"::integer
FROM "Story"
WHERE "recommendationIndex" is not null
    and "lang" = 'ru'
    and "recommendationIndex" < 120
order by "recommendationIndex" asc