WITH src as (
    SELECT * FROM
    {{ source('datagouv', 'communes') }}
)
SELECT
    codgeo as code_insee,
    libgeo as nom_commune,
    dep as departement,
    reg as region,
    type_commune_uu as type_commune
    FROM src