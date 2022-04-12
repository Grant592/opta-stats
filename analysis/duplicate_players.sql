select
     *,
     count(*) over (partition by plid) as row_num 
from {{ ref ('stg_players') }}
qualify row_number() over (partition by plid) > 1
order by plid