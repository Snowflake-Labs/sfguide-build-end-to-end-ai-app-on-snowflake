-- Monthly Cohort Analysis
-- Tracks customer cohorts by signup month and analyzes retention

with customers as (
    select
        customer_id,
        signup_date,
        signup_month
    from {{ ref('stg_customers') }}
),

orders as (
    select
        customer_id,
        order_date,
        order_month,
        total_amount
    from {{ ref('stg_orders') }}
    where lower(order_status) in ('completed', 'shipped', 'delivered')
),

-- Define cohorts by signup month
cohorts as (
    select
        c.signup_month as cohort_month,
        count(distinct c.customer_id) as cohort_size,
        min(c.signup_date) as cohort_start_date
    from customers c
    group by c.signup_month
),

-- Customer activity by month
customer_activity as (
    select
        c.customer_id,
        c.signup_month as cohort_month,
        o.order_month as activity_month,
        datediff('month', c.signup_month, o.order_month) as months_since_signup,
        count(distinct o.order_date) as orders_in_month,
        sum(o.total_amount) as revenue_in_month
    from customers c
    join orders o on c.customer_id = o.customer_id
    group by 
        c.customer_id,
        c.signup_month,
        o.order_month
),

-- Cohort metrics by period
cohort_metrics as (
    select
        ca.cohort_month,
        ca.months_since_signup,
        c.cohort_size,
        
        -- Retention metrics
        count(distinct ca.customer_id) as active_customers,
        count(distinct ca.customer_id) * 1.0 / nullif(c.cohort_size, 0) as retention_rate,
        
        -- Revenue metrics
        sum(ca.revenue_in_month) as cohort_revenue,
        sum(ca.revenue_in_month) / nullif(count(distinct ca.customer_id), 0) as revenue_per_active_customer,
        sum(ca.revenue_in_month) / nullif(c.cohort_size, 0) as revenue_per_cohort_member,
        
        -- Activity metrics
        sum(ca.orders_in_month) as total_orders,
        sum(ca.orders_in_month) * 1.0 / nullif(count(distinct ca.customer_id), 0) as orders_per_active_customer
        
    from customer_activity ca
    join cohorts c on ca.cohort_month = c.cohort_month
    group by 
        ca.cohort_month,
        ca.months_since_signup,
        c.cohort_size
),

-- Calculate cohort health indicators
final as (
    select
        cohort_month,
        months_since_signup,
        cohort_size,
        active_customers,
        retention_rate,
        cohort_revenue,
        revenue_per_active_customer,
        revenue_per_cohort_member,
        total_orders,
        orders_per_active_customer,
        
        -- Cumulative metrics
        sum(cohort_revenue) over (
            partition by cohort_month 
            order by months_since_signup
        ) as cumulative_revenue,
        
        -- Churn rate (compared to previous month)
        lag(retention_rate) over (
            partition by cohort_month 
            order by months_since_signup
        ) - retention_rate as monthly_churn_rate,
        
        -- Cohort health classification
        case
            when retention_rate >= 0.5 then 'healthy'
            when retention_rate >= 0.3 then 'moderate'
            when retention_rate >= 0.1 then 'at_risk'
            else 'poor'
        end as cohort_health,
        
        -- LTV estimation
        sum(cohort_revenue) over (
            partition by cohort_month 
            order by months_since_signup
        ) / nullif(cohort_size, 0) as estimated_ltv_to_date
        
    from cohort_metrics
)

select * from final
order by cohort_month desc, months_since_signup
