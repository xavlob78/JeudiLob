"""
Pipeline dlt : exécute une requête SQL sur PostgreSQL et charge le résultat dans DuckDB.

Secrets (.dlt/secrets.toml)
  [pg_to_duck.postgres] credentials = "postgresql://user:pass@host:5432/db"
  (repli : [meteo.postgres] credentials)

Config (.dlt/config.toml)
  [sources.pg_to_duck]
    sql = "SELECT id, nom FROM ..."   — requête obligatoire (colonnes = champs des lignes)
    resource_name = "ma_table"       — nom de la table dans DuckDB (défaut : query_result)
    write_disposition = "replace"    — replace | append | merge
    primary_key = "id"               — optionnel (une colonne ou liste pour merge)

  [pg_to_duck]
    duckdb_path = "Db/export.duckdb"
    dataset_name = "from_pg"
"""

from __future__ import annotations

from typing import Any, Iterator

import dlt
import psycopg2
from dlt.destinations import duckdb as duckdb_destination


def _postgres_uri() -> str:
    uri = dlt.secrets.get("pg_to_duck.postgres.credentials") or dlt.secrets.get(
        "meteo.postgres.credentials"
    )
    if not uri:
        raise ValueError(
            "Définir [pg_to_duck.postgres] credentials dans .dlt/secrets.toml "
            '(ou [meteo.postgres] credentials) : "postgresql://..."'
        )
    return str(uri)


def _rows_from_postgres(connection_uri: str, sql: str) -> Iterator[dict[str, Any]]:
    """Exécute la requête et produit une ligne = un dict (noms de colonnes PostgreSQL)."""
    conn = psycopg2.connect(connection_uri)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            if cur.description is None:
                return
            colnames = [d[0] for d in cur.description]
            for row in cur:
                yield dict(zip(colnames, row))
    finally:
        conn.close()


def _config_primary_key() -> str | list[str] | None:
    pk = dlt.config.get("sources.pg_to_duck.primary_key")
    if pk is None or pk == "":
        return None
    if isinstance(pk, list):
        return [str(x) for x in pk]
    return str(pk)


@dlt.source(name="pg_to_duck")
def postgres_to_duck_source() -> Any:
    """Source à une ressource : résultat de la requête SQL."""
    sql = dlt.config.get("sources.pg_to_duck.sql")
    if not sql or not str(sql).strip():
        raise ValueError(
            'Configurer [sources.pg_to_duck] sql = "SELECT ..." dans .dlt/config.toml'
        )

    resource_name =  "query_result"
    write_disposition = "replace"
    primary_key = _config_primary_key()

    kwargs: dict[str, Any] = {
        "name": str(resource_name),
        "write_disposition": str(write_disposition),
    }
    if primary_key is not None:
        kwargs["primary_key"] = primary_key

    @dlt.resource(**kwargs)
    def query_result() -> Any:
        yield from _rows_from_postgres(_postgres_uri(), str(sql))

    return [query_result]


def load_postgres_to_duckdb() -> None:
    db_path = "./Db/pg_to_duck.duckdb"
    dataset = "from_pg"
    pipeline = dlt.pipeline(
        pipeline_name="postgres_to_duckdb",
        destination=duckdb_destination(db_path),
        dataset_name=dataset,
        progress="tqdm",
    )
    load_info = pipeline.run(postgres_to_duck_source())
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    load_postgres_to_duckdb()
