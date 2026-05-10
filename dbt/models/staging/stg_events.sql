{{
    config(
        materialized='view',
        tags=['staging', 'events']
    )
}}

-- Staging model: read Gold Iceberg events via Redshift Spectrum external schema
-- Renames, casts, and light-cleans the Gold layer for downstream consumption.

with source as (
    select * from {{ source('iceberg_gold', 'daily_event_summary') }}
),

renamed as (
    select
        event_date,
        event_type,
        event_count,
        unique_users,
        total_amount,
        avg_amount,
        last_event_at,
        _gold_aggregated_at as dbt_loaded_at
    from source
    where event_date >= '{{ var("start_date") }}'
)

select * from renamed
