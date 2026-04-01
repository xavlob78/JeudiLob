WITH 
    datagouv as 
    (
        SELECT * from {{ ref('datagouv_communes') }}
    ),
osm as 
    (
        SELECT * from {{ ref('osm_restau_communes') }}
    )
SELECT 
    COALESCE(datagouv.code_insee,osm.code_insee) as code_insee,
    COALESCE(datagouv.nom_commune,osm.nom_commune) as nom_commune,
    COALESCE(datagouv.departement,osm.departement) as departement,
    osm.code_departement,
    COALESCE(datagouv.region,osm.region) as region,
    osm.code_region,
    COALESCE(datagouv.type_commune,'NA') as type_commune
FROM datagouv
LEFT JOIN osm
    ON datagouv.code_insee = osm.code_insee
