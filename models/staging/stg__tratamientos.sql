with
    final as (
        select tratamiento, categoria_tratamiento, tratamiento_origen
        from {{ source("google_sheets", "tratamientos") }}
    )
select *
from final
