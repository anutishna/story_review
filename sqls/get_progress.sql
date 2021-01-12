WITH progs_data AS
  (SELECT "userId",
          ("progress"->>'finishedEpisodes')::integer AS fin_eps,
                       to_timestamp(("progress"->>'updatedAt')::integer)::date AS updated_at,
                       i."deviceType"
   FROM "UserProgress" up
   JOIN "Installation" i ON i."user" = up."userId"
   WHERE "storyId" = '{{story_id}}'
   ORDER BY updated_at ASC nulls LAST)
SELECT updated_at,
       fin_eps,
       count(DISTINCT "userId") AS users_read,
       count(DISTINCT "userId") FILTER (
                                        WHERE "deviceType" = 'ios') AS ios_users_read,
       count(DISTINCT "userId") FILTER (
                                        WHERE "deviceType" = 'android') AS android_users_read
FROM progs_data
GROUP BY updated_at,
         fin_eps
ORDER BY updated_at ASC nulls LAST