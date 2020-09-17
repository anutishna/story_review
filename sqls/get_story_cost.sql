WITH status_data AS
  (SELECT s.title,
          s.author,
          s."cardId",
          ((s."assigneeIds"::jsonb->-1)::jsonb->'assignee')::text AS editor_id,
          unnest(s."labels") AS label,
          s."episodesCount",
          s."episodesCountFinished",
          h."addedAt" AS status_date,
          (CASE
               WHEN h."status" = 'approved' THEN 'edited'
               ELSE h."status"
           END) AS status
   FROM "StoryStatusHistory" h
   LEFT OUTER JOIN "StoryRegistry" s ON s."id" = h."storyId"
   WHERE h."storyId" IS NOT NULL
     AND h."status" = 'approved' ),
     editor_data AS
  (SELECT "cardId",
          status_date::date AS edited_at,
          (CASE
               WHEN "editor_id" = '"5a9d417c66dd552a6515c6f9"' THEN 'Наталья Ш.'
               WHEN "editor_id" = '"5db96120b8a2476cb05545fe"' THEN 'Евгения М.'
               WHEN "editor_id" = '"5d67590420902027089e5879"' THEN 'Галина'
               WHEN "editor_id" = '"5af586468e6714f70f0cf1cc"' THEN 'Зарина'
               WHEN "editor_id" = '"5af948e7c6d0615ad213ed30"' THEN 'Юлия'
               WHEN "editor_id" = '"5b8e47f77861bc659265a754"' THEN 'Софья (Anonche)'
               WHEN "editor_id" = '"5cdaac79738d7b5bc46f1705"' THEN 'Алина (Eva)'
               WHEN "editor_id" = '"5cd56270d5aad024b703e44e"' THEN 'София Л.'
               WHEN "editor_id" = '"5b0bdadb1c882bc2efd25899"' THEN 'Катя У.'
               ELSE "editor_id"
           END) AS "editor_name",
          "title",
          "author",
          "episodesCountFinished" AS edited_episodes,
          (CASE
               WHEN "label" = 'Работа над сюжетом, корректура' THEN 300
               WHEN "label" = 'Шлифовка сюжета, корректура' THEN 250
               WHEN "label" = 'Корректура' THEN 150
               ELSE 250
           END) AS price,
          "episodesCount" AS author_episodes
   FROM status_data) --  final_data AS

SELECT *,
       (CASE
            WHEN edited_episodes = 0 THEN author_episodes*price
            ELSE edited_episodes*price
        END) AS story_cost
FROM editor_data
WHERE "title" = '{{title}}'
ORDER BY edited_at ASC