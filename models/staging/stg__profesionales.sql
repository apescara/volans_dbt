with
    final as (
        select profesional, categoria_profesional
        from {{ source("google_sheets", "profesionales") }}
    )
select *
from final
