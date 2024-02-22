{{
    config(
        materialized='table'
    )
}}

with fhv as (
    select *, 
    from {{ ref('stg_fhv') }}
    where pickup_location_id is not null
    and dropoff_location_id is not null

), dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 
    fhv.*
from fhv
inner join dim_zones as pickup_zone
on fhv.pickup_location_id = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv.dropoff_location_id = dropoff_zone.locationid