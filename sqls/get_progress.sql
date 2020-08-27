WITH progs_data AS
  ( SELECT "userId",
           "progress"->>'finishedEpisodes' AS fin_eps,
                        to_timestamp(("progress"->>'updatedAt')::integer)::date AS updated_at
   FROM "UserProgress"
   WHERE "storyId" = '{{story_id}}'
   ORDER BY updated_at ASC nulls LAST )
SELECT updated_at,
       fin_eps,
       count(DISTINCT "userId") AS users_read
FROM progs_data
GROUP BY updated_at,
         fin_eps
ORDER BY updated_at ASC nulls LAST