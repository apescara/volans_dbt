with
    base as (select * from {{ ref("stg__pacientes") }}),
    citas as (
        select distinct
            id_paciente,
            id_cita,
            fecha_creacion_utc as fecha_creacion_cita,
            referencia_arreglada,
            categoria_tratamiento,
            categoria_profesional,
            paciente_atendido,
            categoria_final
        from {{ ref("citas") }}
        where primera_cita
    ),
    final as (
        select
            base.*,
            id_cita,
            fecha_creacion_cita,
            referencia_arreglada,
            categoria_tratamiento,
            categoria_profesional,
            paciente_atendido,
            id_cita is not null as con_cita,
            (extract(year from fecha_registro) * 100)
            + extract(month from fecha_registro) as mes_registro,
            coalesce(referencia_arreglada, referencia) as referencia_agregada,
            case
                when coalesce(referencia_arreglada, referencia) = "Meta Ads"
                then "Meta Ads"
                when coalesce(referencia_arreglada, referencia) = "Google Ads"
                then "Google Ads"
                when coalesce(referencia_arreglada, referencia) = "WhatsApp"
                then "Google Ads"
                when coalesce(referencia_arreglada, referencia) = "Teléfono"
                then "Google Ads"
                when coalesce(referencia_arreglada, referencia) = "Reserva Online"
                then "Google Ads"
                else "Otros"
            end as referencia_simplificada,
            categoria_final
        from base
        left join citas using (id_paciente)
    )
select *
from final
