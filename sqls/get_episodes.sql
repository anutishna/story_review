SELECT -- если дата релиза эпизода меньше (раньше) или равна дате релиза истории, то датой публикации считать дату релиза истории,
 -- в противном случае датой релиза считать дату релиза эпизода
 (CASE
      WHEN e."releasedAt"::date < s."releasedAt"::date THEN s."releasedAt"::date
      ELSE e."releasedAt"::date
  END) AS "published_at", 
  -- если дата публикации эпизода (позже) больше даты релиза, то эпизод публиковался на раннем доступе
 (CASE
      WHEN e."publishedAt"::date > e."releasedAt"::date THEN e."publishedAt"::date
      ELSE NULL
  END) AS "free_access_at",
  (CASE
      WHEN e."publishedAt"::date > e."releasedAt"::date THEN 'true'
      ELSE 'false'
  END) AS "is_early_access",
 s."title",
 s."textId",
 s."author",
 e."order"::integer AS "ep_num", 
 -- если история помечена как законченная и этот эпизод является последним из залитых, то почечать его как последний (т.е. история закончена)
 (CASE
      WHEN (s."continued" IS NOT TRUE
            AND e."order" = s."totalEpisodesCount") THEN 'true'
      ELSE 'false'
  END) AS "finished",
  s."hasAudio",
  s."episodesCount"
FROM "Episode" e
JOIN "Story" s ON e."story" = s."objectId"
WHERE s."objectId"  = '{{story_id}}'
order by ep_num