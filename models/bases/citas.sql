with
    base as (select * from {{ ref("stg__citas") }}),
    primeras_citas as (
        select
            id_cita,
            rank() over (partition by id_paciente order by fecha asc) as primera_cita
        from {{ ref("stg__citas") }}
    ),
    pacientes_atendidos as (
        select
            id_paciente,
            max(
                if(estado_cita = "Atendido", "Atendido", "No Atendido")
            ) as paciente_atendido
        from {{ ref("stg__citas") }}
        group by id_paciente
    ),
    final as (
        select
            base.*,
            if(primera_cita = 1, true, false) as primera_cita,
            coalesce(categoria_tratamiento, "Sin Tratamiento") as categoria_tratamiento,
            tratamiento_origen as tratamiento_origen,
            coalesce(categoria_profesional, "Sin Profesional") as categoria_profesional,
            paciente_atendido,
            case
                when referencia is not null
                then referencia
                when lower(comentario_cita) like "%reserva online%"
                then "Reserva Online"
                else null
            end as referencia_arreglada,
            if(
                coalesce(categoria_tratamiento, "Sin Tratamiento") = "Sin Tratamiento",
                categoria_profesional,
                categoria_tratamiento
            ) as categoria_final
        from base
        left join primeras_citas using (id_cita)
        left join {{ ref("stg__tratamientos") }} using (tratamiento)
        left join {{ ref("stg__profesionales") }} using (profesional)
        left join pacientes_atendidos using (id_paciente)
    )
select *
from final
