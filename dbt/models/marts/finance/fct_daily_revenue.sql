{{
    config(
        materialized='table',
        tags=['finance', 'daily'],
        sort='event_date',
        dist='event_date'
    )
}}

-- Finance mart: daily revenue metrics by event type.
-- Consumed directly by BI tools / Redshift queries.

with events as (
    select * from {{ ref('stg_events') }}
    where event_type in ('purchase', 'refund', 'subscription')
),

daily_revenue as (
    select
        event_date,
        event_type,
        sum(total_amount)                                        as total_revenue,
        sum(unique_users)                                        as paying_users,
        sum(event_count)                                         as transaction_count,
        safe_divide(sum(total_amount), nullif(sum(unique_users), 0)) as revenue_per_user,

        -- 7-day rolling revenue
        sum(sum(total_amount)) over (
            partition by event_type
            order by event_date
            rows between 6 preceding and current row
        )                                                        as revenue_7d,

        -- Month-to-date revenue
        sum(sum(total_amount)) over (
            partition by event_type, date_trunc('month', event_date)
            order by event_date
            rows unbounded preceding
        )                                                        as revenue_mtd

    from events
    group by 1, 2
)

select * from daily_revenue
