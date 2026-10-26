select
    event_id,
    event_time,
    event_date,
    site_id,
    user_id
from {{ source('silver', 'page_view') }}
