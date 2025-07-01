-- Create the view with improved logic for weekly average and product lifecycle tracking
CREATE OR REPLACE VIEW public.seller_metrics AS

WITH 
-- First, calculate the number of distinct weeks a seller was active
weekly_activity AS (
    SELECT
        p.seller_id,
        COUNT(DISTINCT DATE_TRUNC('week', p.created_at)) AS active_week_count
    FROM
        public.products p
    GROUP BY
        p.seller_id
),

-- Second, get lifetime unique product counts, including active and removed stats
lifetime_products AS (
    SELECT
        p.seller_id,
        COUNT(DISTINCT p.id) AS lifetime_unique_products,
        COUNT(DISTINCT p.id) FILTER (WHERE p.is_removed = false OR p.is_removed IS NULL) AS active_unique_products,
        COUNT(DISTINCT p.id) FILTER (WHERE p.is_removed = true) AS removed_unique_products
    FROM
        public.products p
    GROUP BY
        p.seller_id
),

-- Third, calculate metrics for just the last 7 days
recent_metrics AS (
    SELECT
        p.seller_id,
        COUNT(DISTINCT p.id) AS last_7_days_unique_products
    FROM
        public.products p
    WHERE
        p.created_at >= (NOW() - INTERVAL '7 days')
    GROUP BY
        p.seller_id
)

-- Finally, join all the calculated metrics together
SELECT
    s.id AS seller_id,
    s.name AS seller_name,
    s.is_active,
    
    COALESCE(lp.lifetime_unique_products, 0) AS lifetime_unique_products,
    COALESCE(lp.active_unique_products, 0) AS active_unique_products,
    COALESCE(lp.removed_unique_products, 0) AS removed_unique_products,
    
    -- Safely calculate the average using the count of active weeks
    CASE
        WHEN wa.active_week_count > 0 THEN
            ROUND((COALESCE(lp.lifetime_unique_products, 0)::numeric / wa.active_week_count), 2)
        ELSE 0
    END AS avg_unique_products_per_week,
    
    COALESCE(rm.last_7_days_unique_products, 0) AS last_7_days_unique_products
    
FROM
    public.sellers s
    LEFT JOIN weekly_activity wa ON s.id = wa.seller_id
    LEFT JOIN lifetime_products lp ON s.id = lp.seller_id
    LEFT JOIN recent_metrics rm ON s.id = rm.seller_id
ORDER BY
    s.name;