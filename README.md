Installation 

  1. Python : https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe
  2. pip : https://bootstrap.pypa.io/get-pip.py
  3. Dans un terminal
             python get-pip.py
             pip intall uv
             uv venv --python 3.11
             .\venv\Scripts\activate
             uv pip install "dlt[duckdb]"
             uv pip install dbt-core dbt-duckdb
             uv pip install streamlit
             uv pip install ipykernel
  4. Recuperer un IDE (Cursor / VsCode / WindSurf)
             https://www.cursor.com/downloads
  5. Installer le CLI DuckDb
             https://github.com/duckdb/duckdb/releases/download/v1.2.2/duckdb_cli-windows-amd64.zip

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
    
