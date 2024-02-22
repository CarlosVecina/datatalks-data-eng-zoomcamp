 {{ config(materialized='view') }}

 select
   -- identifiers
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_location_id,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_location_id,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- payment info
    SR_Flag as fare_amount,
    dispatching_base_num as extra,
    Affiliated_base_number as mta_tax,

from {{ source('staging','fhv') }}
where pickup_datetime >= '2019-01-01'
and pickup_datetime > '2012-01-01'