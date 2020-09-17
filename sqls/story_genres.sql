with story_data as(
    select s."releasedAt"::date as released_at, s."title", s."textId" as text_id, s."author", c."textId" as genre
    from "Story" s
        join "_Join:categories:Story" cs on cs."owningId" = s."objectId"
        join "Category" c on cs."relatedId" = c."objectId"
    where s."releasedAt" >= '{{start_date}}'
        and s."releasedAt" <= '{{end_date}}'
        and s."lang" = 'ru'
)
select genre,
    count(distinct text_id) as stories
from story_data
group by genre