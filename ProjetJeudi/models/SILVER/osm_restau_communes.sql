WITH src as (
    SELECT * from {{ source('restos', 'restaurants') }}
)
SELECT DISTINCT 
    com_insee as code_insee,
    com_nom as nom_commune,
    region,
    code_region,
    departement,
    code_departement,
    commune,
    code_commune
FROM src