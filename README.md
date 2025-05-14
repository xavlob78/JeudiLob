

  1. Python : https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe
  2. pip : https://bootstrap.pypa.io/get-pip.py
  3. Dans un terminal
             python get-pip.py
             pip intall uv
             uv venv --python 3.11
             .\venv\Scripts\activate
             uv pip install "dlt[duckdb,snowflake,filesystem,bigquery]"
             uv pip install dbt-core dbt-duckdb
             uv pip install streamlit
             uv pip install ipykernel
  4. Recuperer un IDE (Cursor / VsCode / WindSurf)
             https://www.cursor.com/downloads
  5. Installer le CLI DuckDb
             https://github.com/duckdb/duckdb/releases/download/v1.2.2/duckdb_cli-windows-amd64.zip


6. Initialisation projet dbt
        * dbt init
        * copier le fichier profiles.yml dans le repertoire projet (C:\Users\xxxx\.dbt) .. Attention a le placer dans le .gitignore !
        * configurer le fichier profile suivant le projet
                 JeudiLob:
                      outputs:
                        nba:
                          type: duckdb
                          path: ../Db/nba.db
                          threads: 4

                    target: nba
    * tester l'install avec dbt debug
    * tester la config avec dbt run
          Normalement les tables/vues my_first_dbt_model / my_second_dbt_model doivent être crééees
    * supprimer les tables 
    * supprimer le dossier models/example
  
    * Installer le package codegen
            packages:
              - package: dbt-labs/codegen
                version: 0.13.1

   * essayer le package generate_source
              dbt --quiet run-operation generate_source --args '{"schema_name": "csv","generate_columns":false, "include_data_types":false}' > models/_sources.yml
              Voir le problème (vient de l'adapter duckdb)
              Copier la macro get_tables_by_pattern_sql.sql dans le repertoire macros et modifier le sql (supprimer le {database})

   * permet de créer un fichier _source par schema
   * Ajout d'un source freshness sur la table _dlt_loads

  * Creation Modele Equipe a partir des sources csv.team, csv.team_details, nbaapi.teamdetails
  * Ajout de tests sur le modele genere
          unique et not_null sur les clés
 * Creation de la table Joueurs a partir de csv_player, csv_common_player_info, api.players_details,api.players
    * Ajout de tests sur le modele genere
          unique et not_null sur les clés
* Creation Table Stats_Joueurs (Joueurs, Equipe, api.players) en modele incremental par rapport a dlt_loads

* generation docs : dbt docs generate
* dbt docs serve
   
Données acrelec
  1. doc API : https://boh-lab-br.acrelec.com:9443/mwboh/doc/
Base  SnowFlake cible
  2. Connection
        account = UVBWKDZ-MSC41063 
        user = XAVTHO 
        role = ACCOUNTADMIN 
        database = JEUDILOB
Données NBA
  Fichiers
        https://drive.google.com/file/d/1oZhB0ExbhwXMtAvdmK-649mGcm4488XQ/view?usp=sharing
  API    
        http://rest.nbaapi.com/index.html
    
