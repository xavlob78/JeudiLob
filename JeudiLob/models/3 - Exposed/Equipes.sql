with equ_det as (
    select * from {{ source('nbacsv', 'team_details') }}
),
equ as (
    select * from {{ source('nbacsv', 'team') }}
),
api_team as (
    select * from {{ source('nbaapi', 'teams_details') }}
)
select 
    a.id IdEquipe,
    c.team_id IdEquipeNBA, 
    coalesce(a.full_name, c.name) NomEquipe,
    a.abbreviation AbbrevEquipe, 
    a.city VilleEquipe, 
    a.state EtatEquipe, 
    b.arena NomArene,
    b.arenacapacity CapaciteArene,
    c.conference Conference,
    c.division Division, 
    c.head_coach EntraineurActuel
from equ a
inner join equ_det b on a.id = b.team_id
full join api_team c on a.id = c.nba_dot_com_team_id




