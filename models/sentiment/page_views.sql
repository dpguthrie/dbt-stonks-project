select
    date_day,
    page_title,
    total_page_views
from {{ ref('tpch', 'agg_segment_page_views') }}
where page_source = 'yahooquery'