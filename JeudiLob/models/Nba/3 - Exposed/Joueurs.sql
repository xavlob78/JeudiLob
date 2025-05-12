with csv_player as (
    select * from {{ source('nbacsv', 'player') }}
),

csv_common_player_info as (
    select * from {{ source('nbacsv', 'common_player_info') }}
),

join_api as (
  with unpiv as (
    unpivot {{ source('nbaapi', 'players_details') }}
    ON fan_duel_name, draft_kings_name, yahoo_name, fantasy_draft_name
    INTO 
    NAME name
    VALUE player_name
  )
select distinct replace(player_name,'*','') player_name, player_id
from unpiv
),

det_api as (
  select distinct player_id, replace(player_name,'*','') as player_name
  from {{ source('nbaapi', 'players') }}
)


SELECT 
    csv_player.id as Id,
    csv_player.full_name as NomComplet,
    csv_player.first_name as Prenom,
    csv_player.last_name as Nom,
    csv_player.is_active as EstActif,
    csv_common_player_info.birthdate as DateDeNaissance,
    csv_common_player_info.school as Universite,
    csv_common_player_info.country as Pays,
    csv_common_player_info.draft_year as AnneeDeDraft,
    csv_common_player_info.height as Taille,
    csv_common_player_info.weight as Poids,
    csv_common_player_info.season_exp as Experience,
    join_api.player_id as IdApi,
    det_api.player_id as CodeApi
from csv_player 
inner join csv_common_player_info on csv_player.id = csv_common_player_info.person_id
left join join_api on csv_player.full_name = join_api.player_name
left join det_api on join_api.player_name = det_api.player_name
