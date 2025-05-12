with api_players as (
    select * from {{ source('api', 'players') }}
),

api_players_details as (
    select * from {{ source('api', 'players_details') }}
),

csv_player as (
    select * from {{ source('files', 'player') }}
),

csv_common_player_info as (
    select * from {{ source('files', 'common_player_info') }}
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
    csv_common_player_info.position as Position,
    csv_common_player_info.season_exp as Expérience,
    csv_common_player_info.draft_year as AnneeDeDraft
from csv_player 
inner join csv_common_player_info on csv_player.id = csv_common_player_info.person_id
    
    
