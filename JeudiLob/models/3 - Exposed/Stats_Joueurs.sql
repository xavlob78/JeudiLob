{{
  config(
    materialized = 'incremental',
    unique_key = ['IdJoueur', 'IdEquipe', 'Season'],
  )
}}

with players as (
    select * from {{ ref('Joueurs') }}
),
team as (
    select * from {{ ref('Equipes') }}
),
src as (
    select * from {{ source('nbaapi', 'players') }}
),
load as (
    select * from {{ source('nbaapi', '_dlt_loads') }}
)
select
   players.id as IdJoueur,
   team.IdEquipe,
   src.season,  
   src.position,
   src.games, 
   src.minutes_pg, 
   src.field_goals,
   src.field_attempts, 
   src.three_fg, 
   src.three_attempts,
   src.two_fg, 
   src.two_attempts, 
   src.total_rb,
   src.assists,
   src.steals,blocks,
   src.turnovers, 
   src.personal_fouls,
   load.inserted_at as Creele
FROM src
inner join players on src.player_id = players.CodeApi
inner join team on src.team = team.AbbrevEquipe
inner join load on src._dlt_load_id = load.load_id

{% if is_incremental() %}
        where load.inserted_at >= 
         (select coalesce(max(CreeLe),'1900-01-01') from {{ this }} )
{% endif %}