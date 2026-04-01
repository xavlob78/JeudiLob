"""
Pipeline dlt : référentiel géographique français (data.gouv.fr) → PostgreSQL.

Jeu de données : « Référentiel géographique français, communes, … »
Ressource JSON : a1475d8d-e4a4-48a6-8287-f79d21c57904
https://www.data.gouv.fr/datasets/referentiel-geographique-francais-communes-unites-urbaines-aires-urbaines-departements-academies-regions

Les enregistrements OpenDataSoft (fields + recordid) sont aplatis en une seule table.

Secrets : [referentiel_geo.postgres] credentials (ou repli sur [meteo.postgres]).
Config : [sources.referentiel_geo], [referentiel_geo.postgres]
"""

from __future__ import annotations

from typing import Any

import dlt
import requests
from dlt.destinations import postgres as postgres_destination

DEFAULT_JSON_URL = (
    "https://data.enseignementsup-recherche.gouv.fr/explore/dataset/"
    "fr-esr-referentiel-geographique/download/?format=json&timezone=Europe/Berlin"
)


def _flatten_ods_record(rec: dict[str, Any]) -> dict[str, Any]:
    """Aplatit un enregistrement export API explore (datasetid, recordid, fields, geometry)."""
    row: dict[str, Any] = {
        "datasetid": rec.get("datasetid"),
        "recordid": rec.get("recordid"),
        "record_timestamp": rec.get("record_timestamp"),
    }
    for k, v in (rec.get("fields") or {}).items():
        row[k] = v
    geom = rec.get("geometry")
    if geom is not None:
        row["_geometry"] = geom
    return row


def _postgres_credentials() -> str:
    creds = dlt.secrets.get("referentiel_geo.postgres.credentials") or dlt.secrets.get(
        "meteo.postgres.credentials"
    )
    if not creds:
        raise ValueError(
            "Définir [referentiel_geo.postgres] credentials dans .dlt/secrets.toml "
            '(ou [meteo.postgres] credentials) : "postgresql://..."'
        )
    return creds


@dlt.source(name="referentiel_geo")
def referentiel_geo_source() -> Any:
    """Charge le JSON du référentiel (liste d’objets ODS) en une table."""
    url = dlt.config.get("sources.referentiel_geo.json_url") or DEFAULT_JSON_URL
    timeout = dlt.config.get("sources.referentiel_geo.request_timeout_seconds") or 600

    @dlt.resource(
        name="referentiel_geographique",
        write_disposition="replace",
        primary_key="recordid",
    )
    def referentiel_geographique() -> Any:
        r = requests.get(str(url), timeout=float(timeout))
        r.raise_for_status()
        payload = r.json()
        if not isinstance(payload, list):
            raise TypeError(f"Réponse JSON attendue : liste, obtenu {type(payload).__name__}")
        for rec in payload:
            if isinstance(rec, dict):
                yield _flatten_ods_record(rec)

    return [referentiel_geographique]


def load_referentiel_geo_postgres() -> None:
    dataset = (
        dlt.config.get("referentiel_geo.postgres.dataset_name")
        or dlt.config.get("sources.referentiel_geo.dataset_name")
        or "referentiel_geo"
    )
    
    pipeline = dlt.pipeline(
        pipeline_name="referentiel_geo_postgres",
        destination=postgres_destination(_postgres_credentials()),
        dataset_name=dataset,
        progress="tqdm",
    )
    load_info = pipeline.run(referentiel_geo_source())
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    load_referentiel_geo_postgres()
