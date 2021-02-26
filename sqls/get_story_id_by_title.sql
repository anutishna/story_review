SELECT "objectId" AS story_id
FROM "Story"
WHERE "title" = '{{title}}'
AND "isAudioPerformance" IS NOT TRUE