with
    final as (
        select
            datetime(fecha_carga, "America/Santiago") as fecha_carga,
            nullif(ficha, "nan") as ficha,
            nullif(rut, "nan") as rut,
            nullif(tipo_de_identificacion, "nan") as tipo_de_identificacion,
            safe_cast(extranjero as bool) as extranjero,
            nullif(nombre, "nan") as nombre,
            nullif(apellido_paterno, "nan") as apellido_paterno,
            nullif(apellido_materno, "nan") as apellido_materno,
            cast(safe_cast(id_paciente as numeric) as int64) as id_paciente,
            nullif(sexo, "nan") as sexo,
            safe.parse_date('%d-%m-%Y', fecha_nacimiento) as fecha_nacimiento,
            nullif(correo_electronico, "nan") as correo_electronico,
            nullif(telefono, "nan") as telefono,
            nullif(telefono_movil, "nan") as telefono_movil,
            nullif(pais, "nan") as pais,
            nullif(region, "nan") as region,
            nullif(comuna, "nan") as comuna,
            nullif(direccion, "nan") as direccion,
            safe_cast(puntaje as numeric) as puntaje,
            nullif(profesional, "nan") as profesional,
            nullif(referencia, "nan") as referencia,
            nullif(categoria, "nan") as categoria,
            safe_cast(saldo as numeric) as saldo,
            nullif(prevision, "nan") as prevision,
            safe_cast(puntos_fidelizacion as numeric) as puntos_fidelizacion,
            nullif(antecedentes_personales, "nan") as antecedentes_personales,
            safe.parse_date('%d-%m-%Y', fecha_registro) as fecha_registro,
            nullif(local, "nan") as local,
            nullif(convenio, "nan") as convenio,
            rank() over (
                partition by cast(safe_cast(id_paciente as numeric) as int64)
                order by fecha_carga desc
            ) as r
        from {{ source("data_base", "pacientes") }}
    )
select * except (r)
from final
where r = 1
