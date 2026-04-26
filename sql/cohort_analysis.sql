WITH first_purchase_date AS (
    SELECT 
        c.customer_unique_id,
        DATE_TRUNC('month', o.order_purchase_timestamp) AS order_month,
        DATE_TRUNC('month',
            MIN(o.order_purchase_timestamp) OVER (PARTITION BY c.customer_unique_id)
        ) AS first_buy_month
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o USING(customer_id)
    WHERE o.order_status = 'delivered'
),

diff_month_buy AS (
    SELECT 
        customer_unique_id, 
        order_month, 
        first_buy_month,
    DATE_DIFF('month', first_buy_month, order_month) as cohort_index
FROM first_purchase_date
)

SELECT 
    first_buy_month, 
    cohort_index, 
    COUNT(DISTINCT customer_unique_id) AS users_count
FROM diff_month_buy
GROUP BY first_buy_month, cohort_index
ORDER BY first_buy_month, cohort_index
;
