select * 
from {{ source('source_citi_bike', 'trips') }}
where [Trip Duration] <=0