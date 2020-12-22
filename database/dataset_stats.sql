select
    count(distinct(player_id)),
    count(distinct(player_name)),
    count(distinct(first_name)),
    count(distinct(last_name)),
    count(distinct(position)),
    count(distinct(nationality)),
    count(distinct(team_id)),
    count(distinct(birth_date)),
    count(distinct(birth_place)),
    count(distinct(birth_country)),
    count(distinct(height)),
    count(distinct(weight)),
    count(distinct(rating))
FROM player;

SELECT
    count(distinct(team_id)),
    count(distinct(name)),
    count(distinct(code)),
    count(distinct(country)),
    count(distinct(is_national)),
    count(distinct(founded)),
    count(distinct(venue_name)),
    count(distinct(venue_surface)),
    count(distinct(venue_address)),
    count(distinct(venue_city)),
    count(distinct(venue_capacity))
FROM
    team;

SELECT
     count(distinct(player_id)),
    count(distinct(team_id)),
    count(distinct(league)),
    count(distinct(season)),
    count(distinct(injured)),
    count(distinct(rating)),
    count(distinct(captain)),
    count(distinct(game_appearences)),
    count(distinct(games_minutes_played)),
    count(distinct(games_lineups)),
    count(distinct(substitutes_in)),
    count(distinct(substitutes_out)),
    count(distinct(substitutes_bench)),
    count(distinct(goals_total)),
    count(distinct(assists)),
    count(distinct(conceded)),
    count(distinct(saves)),
    count(distinct(passes_total)),
    count(distinct(key_pass)),
    count(distinct(pass_accuracy)),
    count(distinct(tackles_total)),
    count(distinct(tackles_blocks)),
    count(distinct(tackles_interceptions)),
    count(distinct(duels_total)),
    count(distinct(duels_won)),
    count(distinct(dribbles_attempts)),
    count(distinct(dribbles_success)),
    count(distinct(fouls_drawn)),
    count(distinct(fouls_commited)),
    count(distinct(shots_total)),
    count(distinct(shots_on)),
    count(distinct(penalty_won)),
    count(distinct(penalty_commited)),
    count(distinct(penalty_success)),
    count(distinct(penalty_missed)),
    count(distinct(penalty_saved)),
    count(distinct(yellow_card)),
    count(distinct(yellowred)),
    count(distinct(red_card))
FROM
    player_stats;
