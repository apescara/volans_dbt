with
    final as (
        select
            datetime(fecha_carga, "America/Santiago") as fecha_carga,
            cast(safe_cast(id_cita as numeric) as int64) as id_cita,
            datetime(fecha) as fecha,
            nullif(agenda, "nan") as agenda,
            nullif(profesional, "nan") as profesional,
            nullif(hora_inicio_cita, "nan") as hora_inicio_cita,
            datetime(concat(left(fecha, 11), hora_inicio_cita)) as fecha_inicio_cita,
            nullif(hora_fin_cita, "nan") as hora_fin_cita,
            datetime(concat(left(fecha, 11), hora_fin_cita)) as fecha_fin_cita,
            nullif(tratamiento, "nan") as tratamiento,
            nullif(codigo, "nan") as codigo,
            nullif(rut, "nan") as rut,
            nullif(paciente, "nan") as paciente,
            cast(safe_cast(id_paciente as numeric) as int64) as id_paciente,
            nullif(telefonos, "nan") as telefonos,
            nullif(mail, "nan") as mail,
            nullif(estado_cita, "nan") as estado_cita,
            nullif(estado_pago, "nan") as estado_pago,
            nullif(comentario_cita, "nan") as comentario_cita,
            nullif(categoria_paciente, "nan") as categoria_paciente,
            nullif(referencia, "nan") as referencia,
            nullif(usuario_creacion, "nan") as usuario_creacion,
            datetime(fecha_creacion_utc) as fecha_creacion_utc,
            nullif(prevision, "nan") as prevision,
            nullif(online, "nan") as online,
            nullif(simbolos, "nan") as simbolos,
            nullif(sucursal, "nan") as sucursal,
            nullif(venta_id_de_id_cita, "nan") as venta_id_de_id_cita,
            nullif(ficha, "nan") as ficha,
            nullif(origen, "nan") as origen,
            nullif(sexo, "nan") as sexo,
            nullif(fecha_nacimiento, "nan") as fecha_nacimiento,
            safe_cast(edad as numeric) as edad,
            rank() over (
                partition by cast(safe_cast(id_cita as numeric) as int64)
                order by fecha_carga desc
            ) as r
        from {{ source("data_base", "citas") }}
    )
select * except (r)
from final
where r = 1
