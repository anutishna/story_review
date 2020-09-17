with story_data as(
    select "title", "episodesCount", "status", "createdAt",
        (case 
            when status = 'published' or status = 'in-editing' or status = 'scheduled' or status = 'approved-for-editing'
                or status = 'approved' then 'true'
            else null end) as approved_stories,
        (case 
            when status = 'rejected' or status = 'rejected_as_stolen' then 'true'
            else null end) as rejected_stories,
        (case 
            when status = 'in-inbox' or status = 'in-progress' then 'true'
            else null end) as not_reviewed_stories,
        (case 
            when status = 'rejected_by_ai' then 'true'
            else null end) as rejected_by_ai_stories,
        (case
            when status = 'published' or status = 'in-editing' or status = 'scheduled' or status = 'approved-for-editing'
                or status = 'approved' then "episodesCount"
            else null end) as approved_episodes,
        (case 
            when status = 'rejected' or status = 'rejected_as_stolen' then "episodesCount"
            else null end) as rejected_episodes,
        (case 
            when status = 'in-inbox' or status = 'in-progress' then "episodesCount"
            else null end) as not_reviewed_episodes,
        (case 
            when status = 'rejected_by_ai' then "episodesCount"
            else null end) as rejected_by_ai_episodes
    from "StoryRegistry"
    where "createdAt"::date >= '{{start_date}}' and "createdAt"::date <= '{{end_date}}'
)
-- data_by_date as(
    select
        "createdAt"::date as "Date",
        
        count("title") as "Total Stories",
        count("title") - count("rejected_by_ai_stories") as "Stories to Review",
        count("approved_stories") as "Approved Stories",
        count("rejected_stories") as "Rejected Stories",
        count("not_reviewed_stories") as "Not Reviewed Stories",
        count("rejected_by_ai_stories") as "AI Rejected Stories",
        
        sum("episodesCount") as "Total Episodes",
        sum("episodesCount") - sum("rejected_by_ai_episodes") as "Episodes to Review",
        sum("approved_episodes") as "Approved Episodes",
        sum("rejected_episodes") as "Rejected Episodes",
        sum("not_reviewed_episodes") as "Not Reviewed Episodes",
        sum("rejected_by_ai_episodes") as "AI Rejected Episodes"
    from story_data
    where "createdAt"::date >= '{{start_date}}' and "createdAt"::date <= '{{end_date}}'
    group by "Date"
    order by "Date" asc
-- )
-- select
--     sum("Total Stories")::integer as "Total Stories",
--     sum("Stories to Review")::integer as "Stories to Review",
--     sum("Approved Stories")::integer as "Approved Stories",
--     sum("Rejected Stories")::integer as "Rejected Stories",
--     sum("Not Reviewed Stories")::integer as "Not Reviewed Stories",
--     sum("AI Rejected Stories")::integer as "AI Rejected Stories",
    
--     sum("Total Episodes")::integer as "Total Episodes",
--     sum("Episodes to Review")::integer as "Episodes to Review",
--     sum("Approved Episodes")::integer as "Approved Episodes",
--     sum("Rejected Episodes")::integer as "Rejected Episodes",
--     sum("Not Reviewed Episodes")::integer as "Not Reviewed Episodes",
--     sum("AI Rejected Episodes")::integer as "AI Rejected Episodes"
-- from data_by_date