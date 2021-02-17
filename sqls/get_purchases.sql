WITH gen_data AS
  (SELECT ip."userId",
          pr."receiptId",
          pr."productId",
          pr."type",
          pr."vendor",
          pr."isTrialPeriod",
          pr."count",
          pr."purchaseDate"::date,
          pr."expiresDate"::date,
          (CASE
               WHEN pr."productId" = 'me.zahleb.subscriptions.weekly_paid_trial'
                    AND pr."isTrialPeriod" = 'true' THEN 15
               WHEN pr."productId" = 'me.zahleb.purchases.gems.20' THEN 75
               WHEN pr."productId" = 'me.zahleb.purchases.gems.60' THEN 169
               WHEN pr."productId" = 'me.zahleb.purchases.gems.250' THEN 699
               WHEN pr."productId" = 'me.zahleb.purchases.gems.550' THEN 1490
               WHEN pr."productId" = 'me.zahleb.purchases.gems.1500' THEN 3790
               WHEN pr."productId" = 'me.zahleb.subscriptions.weekly'
                    OR pr."productId" = 'me.zahleb.subscriptions.weekly_alt1'
                    OR pr."productId" = 'me.zahleb.subscriptions.weekly_alt2'
                    OR pr."productId" = 'me.zahleb.subscriptions.weekly_paid_trial' THEN 269*pr."count"
               WHEN pr."productId" = 'me.zahleb.subscriptions.monthly'
                    OR pr."productId" = 'me.zahleb.subscriptions.monthly_alt1'
                    OR pr."productId" = 'me.zahleb.subscriptions.monthly_alt2'
                    OR pr."productId" = 'me.zahleb.subscriptions.3months' THEN 999*pr."count"
               WHEN pr."productId" = 'me.zahleb.subscriptions.yearly' THEN 2690*pr."count"
               WHEN pr."productId" = 'me.zahleb.purchases.lifetime' THEN 3990
               WHEN pr."productId" = 'early_access' THEN 29
               WHEN pr."productId" = 'month_300'
                    AND pr."isTrialPeriod" = 'false' THEN 299
               WHEN pr."productId" = 'month_300'
                    OR pr."productId" = 'month_sale' THEN 299*pr."count"
               WHEN pr."productId" = 'week_sale' THEN 99*pr."count"
               ELSE NULL
           END) * 0.632 AS revenue
   FROM "InAppPurchase" ip
   JOIN "Product" pr ON pr."originalTransactionId" = ip."transactionId"
   WHERE ("currentStoryId" = '{{story_id}}'
          OR "lastStoryId" = '{{story_id}}')
   ORDER BY pr."purchaseDate")
SELECT "purchaseDate",
       count(DISTINCT "userId") AS paid_users,
       count(DISTINCT "receiptId") AS all_purs,
       count(DISTINCT "receiptId") filter (
                                         WHERE TYPE = 'subscription'
                                           AND vendor = 'apple') AS ios_subs,
       sum(CASE
               WHEN TYPE = 'subscription'
                    AND vendor = 'apple' THEN COUNT
               ELSE 0
           END) AS ios_rebills,
       count(DISTINCT "receiptId") filter (
                                         WHERE TYPE = 'subscription'
                                           AND vendor = 'google') AS android_subs,
       sum(CASE
               WHEN TYPE = 'subscription'
                    AND vendor = 'google' THEN COUNT
               ELSE 0
           END) AS android_rebills,
       count(DISTINCT "receiptId") filter (
                                         WHERE "productId" = 'early_access') AS early_accesses,
       sum(revenue) AS total_revenue,
       sum(CASE
               WHEN TYPE = 'subscription'
                    AND vendor = 'apple' THEN revenue
               ELSE 0
           END) AS ios_subs_revenue,
       sum(CASE
               WHEN TYPE = 'subscription'
                    AND vendor = 'google' THEN revenue
               ELSE 0
           END) AS android_subs_revenue,
       sum(CASE
               WHEN "productId" = 'early_access' THEN revenue
               ELSE 0
           END) AS early_access_revenue
FROM gen_data
GROUP BY "purchaseDate"