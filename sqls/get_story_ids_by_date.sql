SELECT "objectId" AS story_id, "title", "author", "contentType", "lang", "releasedAt"::date as publication_date
FROM "Story"
WHERE "releasedAt"::date >= '{{start_date}}'::date
  AND "releasedAt"::date <= '{{end_date}}'::date
  AND "lang" = 'ru'
  AND "isAudioPerformance" IS NOT TRUE
  AND "contentType" <> 'podcast'