with final as (
    SELECT
    datetime(fecha_carga,"America/Santiago") as fecha_carga,
  nullif(ficha,"nan") as ficha,
  nullif(rut,"nan") as rut,
  nullif(tipo_de_identificacion,"nan") as tipo_de_identificacion,
  safe_cast(extranjero as bool) as extranjero,
  nombre,
  apellido_paterno,
  apellido_materno,
  cast(safe_cast(id_paciente as numeric) as int64) as id_paciente,
  sexo,
  SAFE.PARSE_DATE('%d-%m-%Y',  fecha_nacimiento) as fecha_nacimiento,
  correo_electronico,
  telefono,
  telefono_movil,
  pais,
  region,
  comuna,
  direccion,
  safe_cast(puntaje as numeric) as puntaje,
  profesional,
  referencia,
  categoria,
  safe_cast(saldo as numeric) as saldo,
  prevision,
  safe_cast(puntos_fidelizacion as numeric) as puntos_fidelizacion,
  antecedentes_personales,
  SAFE.PARSE_DATE('%d-%m-%Y',  fecha_registro) as fecha_registro,
  local,
  convenio
FROM
  {{ source('data_base', 'pacientes') }}
WHERE
  TIMESTAMP_TRUNC(fecha_carga, DAY) = TIMESTAMP("2025-06-07")
LIMIT
  1000
)
SELECT * from final