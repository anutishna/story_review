-- with prog_data as(
--     select distinct
--         to_timestamp((up."progress"->>'updatedAt')::integer)::date as updated_at
--         , sum(case
--             when (up."progress"->>'finishedEpisodes')::integer is null then s."episodesCount"-1
--             else (up."progress"->>'finishedEpisodes')::integer end)
--                 over (partition by to_timestamp((up."progress"->>'updatedAt')::integer)::date) as fin_eps
--         , sum(case
--             when char_length(u.username) in (25, 36) and (up."progress"->>'listenedEpisodes')::integer is not null
--                 then (up."progress"->>'listenedEpisodes')::integer
--             else 0 end) over (partition by to_timestamp((up."progress"->>'updatedAt')::integer)::date) as ios_listened_eps
--         , sum(case
--             when char_length(u.username) not in (25, 36) and (up."progress"->>'listenedEpisodes')::integer is not null
--                 then (up."progress"->>'listenedEpisodes')::integer
--             else 0 end) over (partition by to_timestamp((up."progress"->>'updatedAt')::integer)::date) as and_listened_eps
--         , sum(case
--             when (up."progress"->>'finishedEpisodes')::integer is null and (up."progress"->>'listenedEpisodes')::integer is null
--                 and char_length(u.username) in (25, 36) then s."episodesCount"-1
--             when char_length(u.username) in (25, 36) and (up."progress"->>'listenedEpisodes')::integer is null
--                 then (up."progress"->>'finishedEpisodes')::integer
--             when char_length(u.username) in (25, 36) and (up."progress"->>'listenedEpisodes')::integer is not null
--                 then (up."progress"->>'finishedEpisodes')::integer - (up."progress"->>'listenedEpisodes')::integer
--             else 0 end) over (partition by to_timestamp((up."progress"->>'updatedAt')::integer)::date) as ios_eps_read
--         , sum(case
--             when (up."progress"->>'finishedEpisodes')::integer is null and (up."progress"->>'listenedEpisodes')::integer is null
--                 and char_length(u.username) not in (25, 36) then s."episodesCount"-1
--             when char_length(u.username) not in (25, 36) and (up."progress"->>'listenedEpisodes')::integer is null
--                 then (up."progress"->>'finishedEpisodes')::integer
--             when char_length(u.username) not in (25, 36) and (up."progress"->>'listenedEpisodes')::integer is not null
--                 then (up."progress"->>'finishedEpisodes')::integer - (up."progress"->>'listenedEpisodes')::integer
--             else 0 end) over (partition by to_timestamp((up."progress"->>'updatedAt')::integer)::date) as and_eps_read
--         , count("userId") over (partition by to_timestamp((up."progress"->>'updatedAt')::integer)::date) as users_stopped_reading
--         , count("userId") filter (where (up."progress"->>'finishedEpisodes')::integer = 0)
--             over (partition by to_timestamp((up."progress"->>'updatedAt')::integer)::date) as users_stopped_at_0
--         , count("userId") filter (where (up."progress"->>'finishedEpisodes')::integer = s."episodesCount"-1
--             or (up."progress"->>'finishedEpisodes')::integer is null)
--             over (partition by to_timestamp((up."progress"->>'updatedAt')::integer)::date) as users_finished_story
--     from "UserProgress" up
--         left join "_User" u on u."objectId" = up."userId"
--         join "Story" s on s."objectId" = up."storyId"
--     where "storyId" = '{{story_id}}'
--         and to_timestamp((up."progress"->>'updatedAt')::integer)::date >= s."releasedAt"::date
--         and to_timestamp((up."progress"->>'updatedAt')::integer)::date <= now()::date
--     order by to_timestamp((up."progress"->>'updatedAt')::integer)::date asc
-- ),
-- purs_data as(
--     select distinct
--         pr."purchaseDate"::date
--         , sum((case
--             when pr."productId" = 'me.zahleb.subscriptions.weekly_paid_trial' and pr."isTrialPeriod" = 'true' then 15
--             when pr."productId" = 'me.zahleb.subscriptions.weekly'
--                 or pr."productId" = 'me.zahleb.subscriptions.weekly_alt1'
--                 or pr."productId" = 'me.zahleb.subscriptions.weekly_alt2'
--                 or pr."productId" = 'me.zahleb.subscriptions.weekly_paid_trial' then 269*pr."count"
--             when pr."productId" = 'me.zahleb.subscriptions.monthly'
--                 or pr."productId" = 'me.zahleb.subscriptions.monthly_alt1'
--                 or pr."productId" = 'me.zahleb.subscriptions.monthly_alt2'
--                 or pr."productId" = 'me.zahleb.subscriptions.3months' then 999*pr."count"
--             when pr."productId" = 'me.zahleb.subscriptions.yearly' then 2690*pr."count"
--             when pr."productId" = 'me.zahleb.purchases.lifetime' then 3990
--         else 0 end) * 0.632) over (partition by pr."purchaseDate"::date) as ios_subs_revenue
--         , sum((case
--             when pr."productId" = 'month_300' and pr."isTrialPeriod" = 'false' then 299
--             when pr."productId" = 'month_300' or pr."productId" = 'month_sale' then 299*pr."count"
--             when pr."productId" = 'week_sale' then 99*pr."count"
--         else 0 end) * 0.632) over (partition by pr."purchaseDate"::date) as android_subs_revenue
--         , sum((case
--             when pr."productId" = 'early_access' then 29
--         else 0 end) * 0.632) over (partition by pr."purchaseDate"::date) as early_access_revenue
--     from "InAppPurchase" ip
--        join "Product" pr on pr."originalTransactionId" = ip."transactionId"
--     where ("currentStoryId" is not null and "currentStoryId" = '{{story_id}}')
--         or ("currentStoryId" is null and "lastStoryId" = '{{story_id}}')
--     order by pr."purchaseDate"::date
-- )
-- select updated_at, fin_eps, ios_listened_eps, and_listened_eps, ios_eps_read, and_eps_read
-- --    , sum(and_eps_read) over (order by updated_at) as and_read_eps_cum
--     , sum(users_stopped_reading) over (order by updated_at) as users_stopped_reading_cum
--     , sum(users_stopped_at_0) over (order by updated_at) as users_stopped_at_0_cum
--     , sum(users_finished_story) over (order by updated_at) as users_finished_story_cum
--     , sum(ios_subs_revenue) over (order by updated_at) as ios_subs_revenue_cum
--     , sum(android_subs_revenue) over (order by updated_at) as android_subs_revenue_cum
--     , sum(early_access_revenue) over (order by updated_at) as early_access_revenue_cum
-- from prog_data
-- left outer join purs_data on purs_data."purchaseDate" = prog_data.updated_at

with prog_data as(
    select distinct
        to_timestamp((up."progress"->>'updatedAt')::integer)::date as updated_at
        
        , sum(case 
            when (up."progress"->>'finishedEpisodes')::integer is null and (up."progress"->>'listenedEpisodes')::integer is null 
                and char_length(u.username) not in (25, 36) then s."episodesCount"-1
            when char_length(u.username) not in (25, 36) and (up."progress"->>'listenedEpisodes')::integer is null 
                then (up."progress"->>'finishedEpisodes')::integer 
            when char_length(u.username) not in (25, 36) and (up."progress"->>'listenedEpisodes')::integer is not null
                then (up."progress"->>'finishedEpisodes')::integer - (up."progress"->>'listenedEpisodes')::integer
            else 0 end) over (partition by to_timestamp((up."progress"->>'updatedAt')::integer)::date) as and_eps_read
            
        , count("userId") over (partition by to_timestamp((up."progress"->>'updatedAt')::integer)::date) as users_stopped
        , count(case when char_length(u.username) not in (25, 36) then "userId" else null end) 
            over (partition by to_timestamp((up."progress"->>'updatedAt')::integer)::date) as and_users_stopped
        , count(case when char_length(u.username) in (25, 36) then "userId" else null end) 
            over (partition by to_timestamp((up."progress"->>'updatedAt')::integer)::date) as ios_users_stopped
            
        , count(case when (up."progress"->>'listenedEpisodes')::integer is null then "userId" else null end) 
            over (partition by to_timestamp((up."progress"->>'updatedAt')::integer)::date) as users_stopped_reading
        , count(case when (up."progress"->>'listenedEpisodes')::integer is not null then "userId" else null end) 
            over (partition by to_timestamp((up."progress"->>'updatedAt')::integer)::date) as users_stopped_listening
        
        , count("userId") filter (where (up."progress"->>'finishedEpisodes')::integer = 0) 
            over (partition by to_timestamp((up."progress"->>'updatedAt')::integer)::date) as users_stopped_at_0
        , count("userId") filter (where (up."progress"->>'finishedEpisodes')::integer = s."episodesCount"-1
            or (up."progress"->>'finishedEpisodes')::integer is null) 
            over (partition by to_timestamp((up."progress"->>'updatedAt')::integer)::date) as users_finished_story
    from "UserProgress" up
        left join "_User" u on u."objectId" = up."userId"
        join "Story" s on s."objectId" = up."storyId"
    where "storyId" = '{{story_id}}'
        and to_timestamp((up."progress"->>'updatedAt')::integer)::date >= s."releasedAt"::date
        and to_timestamp((up."progress"->>'updatedAt')::integer)::date <= now()::date
    order by to_timestamp((up."progress"->>'updatedAt')::integer)::date asc
),
purs_data as(
    select distinct
        pr."purchaseDate"::date
        , sum((case
            when pr."productId" = 'me.zahleb.subscriptions.weekly_paid_trial' and pr."isTrialPeriod" = 'true' then 15
            when pr."productId" = 'me.zahleb.subscriptions.weekly'
                or pr."productId" = 'me.zahleb.subscriptions.weekly_alt1'
                or pr."productId" = 'me.zahleb.subscriptions.weekly_alt2'
                or pr."productId" = 'me.zahleb.subscriptions.weekly_paid_trial' then 269*pr."count"
            when pr."productId" = 'me.zahleb.subscriptions.monthly'
                or pr."productId" = 'me.zahleb.subscriptions.monthly_alt1'
                or pr."productId" = 'me.zahleb.subscriptions.monthly_alt2'
                or pr."productId" = 'me.zahleb.subscriptions.3months' then 999*pr."count"
            when pr."productId" = 'me.zahleb.subscriptions.yearly' then 2690*pr."count"
            when pr."productId" = 'me.zahleb.purchases.lifetime' then 3990
        else 0 end) * 0.632) over (partition by pr."purchaseDate"::date) as ios_subs_revenue
        , sum((case
            when pr."productId" = 'month_300' and pr."isTrialPeriod" = 'false' then 299
            when pr."productId" = 'month_300' or pr."productId" = 'month_sale' then 299*pr."count"
            when pr."productId" = 'week_sale' then 99*pr."count"
        else 0 end) * 0.632) over (partition by pr."purchaseDate"::date) as android_subs_revenue
        , sum((case
            when pr."productId" = 'early_access' then 29
        else 0 end) * 0.632) over (partition by pr."purchaseDate"::date) as early_access_revenue
    from "InAppPurchase" ip
       join "Product" pr on pr."originalTransactionId" = ip."transactionId"
    where ("currentStoryId" is not null and "currentStoryId" = '{{story_id}}')  
        or ("currentStoryId" is null and "lastStoryId" = '{{story_id}}')
    order by pr."purchaseDate"::date
)
select updated_at
    , and_eps_read
    , sum(users_stopped) over (order by updated_at) as users_stopped_cum
    
    , sum(and_users_stopped) over (order by updated_at) as and_users_stopped_cum
    , sum(ios_users_stopped) over (order by updated_at) as ios_users_stopped_cum
    
    , sum(users_stopped_reading) over (order by updated_at) as users_stopped_reading_cum
    , sum(users_stopped_listening) over (order by updated_at) as users_stopped_listening_cum
    
    , sum(users_stopped_at_0) over (order by updated_at) as users_stopped_at_0_cum
    , sum(users_finished_story) over (order by updated_at) as users_finished_story_cum
    , sum(ios_subs_revenue) over (order by updated_at) as ios_subs_revenue_cum
    , sum(android_subs_revenue) over (order by updated_at) as android_subs_revenue_cum
    , sum(early_access_revenue) over (order by updated_at) as early_access_revenue_cum
from prog_data
left outer join purs_data on purs_data."purchaseDate" = prog_data.updated_at