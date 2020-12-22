DROP TABLE stg_dataset_premier_league;

create table tmp_1 as
select /*+ parallel */ * from player_stats minus
            SELECT
                 * 
            FROM
                player_stats
            WHERE
                captain = 0
                AND game_appearences = 0
                AND games_minutes_played = 0
                AND games_lineups = 0
                AND substitutes_in = 0
                AND substitutes_out = 0
                AND substitutes_bench = 0
                AND goals_total = 0
                AND assists = 0
                AND conceded = 0
                AND saves = 0
                AND passes_total = 0
                AND key_pass = 0
                AND pass_accuracy = 0
                AND tackles_total = 0
                AND tackles_blocks = 0
                AND tackles_interceptions = 0
                AND duels_total = 0
                AND duels_won = 0
                AND dribbles_attempts = 0
                AND dribbles_success = 0
                AND fouls_drawn = 0
                AND fouls_commited = 0
                AND shots_total = 0
                AND shots_on = 0
                AND penalty_won = 0
                AND penalty_commited = 0
                AND penalty_success = 0
                AND penalty_missed = 0
                AND penalty_saved = 0
                AND yellow_card = 0
                AND yellowred = 0
                AND red_card = 0;

DROP TABLE stg_dataset_premier_league;

CREATE TABLE stg_dataset_premier_league
    AS WITH q1 AS (
    SELECT DISTINCT
        player_id
    FROM
        player_stats t
    WHERE
        league = 'Premier League'
        AND ( season IN (
            '2015-2016',
            '2016-2017',
            '2017-2018',
            '2018-2019',
            '2019-2020',
            '2014-2015',
            '2013-2014',
            '2012-2013',
            '2011-2012',
            '2010-2011',
            '2009-2010',
            '2008-2009',
            '2007-2008'
        ) )
        AND team_id IN (
            SELECT
                team_id
            FROM
                tmp_premier_league_teams
        )
), q_ps AS (
    SELECT
        *
    FROM
        (
            SELECT
                ROW_NUMBER() OVER(
                    PARTITION BY p.player_id,p.season
                    ORDER BY
                        p.game_appearences DESC, p.games_minutes_played DESC
                ) rn,
                p.*
            FROM
                tmp_1 p
        )
    WHERE
        rn = 1
), q2 AS (
    SELECT
        ps.player_id,
        league,
        team_id,
        season,
        LEAD(league, 1) OVER(
            PARTITION BY ps.player_id
            ORDER BY
                season ASC
        ) l1,
        LEAD(team_id, 1) OVER(
            PARTITION BY ps.player_id
            ORDER BY
                season ASC
        ) t1,
        LEAD(season, 1) OVER(
            PARTITION BY ps.player_id
            ORDER BY
                season ASC
        ) s1,
                LAG(team_id, 1) OVER(
            PARTITION BY ps.player_id
            ORDER BY
                season ASC
        ) lagteam,
        LAG(team_id, 2) OVER(
            PARTITION BY ps.player_id
            ORDER BY
                season ASC
        ) lagteam2,
        LAG(GAMES_MINUTES_PLAYED, 1) OVER(
            PARTITION BY ps.player_id,team_id
            ORDER BY
                season ASC
        ) laggmplay,
        ps.captain,
        ps.game_appearences,
        ps.games_minutes_played,
        ps.games_lineups,
        ps.substitutes_in,
        ps.substitutes_out,
        ps.substitutes_bench,
        ps.goals_total,
        ps.assists,
        ps.conceded,
        ps.saves,
        ps.passes_total,
        ps.key_pass,
        ps.pass_accuracy,
        ps.tackles_total,
        ps.tackles_blocks,
        ps.tackles_interceptions,
        ps.duels_total,
        ps.duels_won,
        ps.dribbles_attempts,
        ps.dribbles_success,
        ps.fouls_drawn,
        ps.fouls_commited,
        ps.shots_total,
        ps.shots_on,
        ps.penalty_won,
        ps.penalty_commited,
        ps.penalty_success,
        ps.penalty_missed,
        ps.penalty_saved,
        ps.yellow_card,
        ps.yellowred,
        ps.red_card
    FROM
        q_ps ps,
        q1
    WHERE
        ps.player_id = q1.player_id
        AND ( season IN (
            '2015-2016',
            '2016-2017',
            '2017-2018',
            '2018-2019',
            '2019-2020',
            '2014-2015',
            '2013-2014',
            '2012-2013',
            '2011-2012',
            '2010-2011',
            '2009-2010',
            '2008-2009',
            '2007-2008'
        ) )
        AND league NOT IN (
            'Cup',
            'Coppa Italia',
            'League Cup',
            'Copa del Rey',
            'UEFA Europa League',
            'FA Cup',
            'UEFA Champions League',
            'UEFA Nations League',
            'DFB Pokal',
            'Coupe de la Ligue',
            'UEFA Super Cup',
            'Schweizer Pokal',
            'KNVB Beker',
            'CONCACAF Champions League',
            'Football League Trophy',
            'Non League Div One',
            'Magyar Kupa',
            'Welsh Cup',
            'CAF Champions League',
            'CAF Super Cup',
            'CAF Confederation Cup',
            'Challenge Cup',
            'FA Trophy',
            'Cupa României',
            'Copa MX',
            'David Kipiani Cup',
            'Scottish Cup',
            'Coupe de France',
            'Coppa Titano',
            'Copa Chile',
            'Suomen Cup',
            'Super Cup',
            'Svenska Cupen',
            'Taça de Portugal',
            'Play-offs 1/2',
            'Recopa Sudamericana',
            'Coupe Nationale',
            'Copa Venezuela',
            'Toto Cup Ligat Al',
            'Cupa',
            'Irish Cup',
            'Presidents Cup'
        )
    ORDER BY
        to_number(player_id),
        season
), q2_1 AS (
    SELECT
        CASE
            WHEN league = 'Premier League' THEN
                1
            ELSE
                0
        END pl,
                CASE WHEN team_id = lagteam then
        1
        else
        0
        end teamexp,
        case when team_id = nvl(lagteam,team_id) and team_id = nvl(lagteam2,team_id) and nvl(lagteam,-1) = nvl(lagteam2,nvl(lagteam,-1)) THEN 0 else 1 end last3SeasonTeamChange,
        case when games_minutes_played<laggmplay*0.8 then -1
        when games_minutes_played>laggmplay*1.2 then 1
        when laggmplay is null then -99
        else 0 end gpmdiffer,
        t.*
    FROM
        q2 t
    ORDER BY
        player_id,
        season
), q2_2 AS (
    SELECT
        CASE
            WHEN ( SUM(pl) OVER(
                PARTITION BY player_id
                ORDER BY
                    player_id, season
            ) - 1 ) < 0 THEN
                0
            ELSE
                ( SUM(pl) OVER(
                    PARTITION BY player_id
                    ORDER BY
                        player_id, season
                ) - 1 )
        END plex,
         SUM(teamexp) OVER(
                PARTITION BY player_id,team_id
                ORDER BY
                    player_id, season
            )+1 sumteamexp,
        t.*
    FROM
        q2_1 t
), q2_3 AS (
    SELECT
        nvl(LAG(pl) OVER(
            PARTITION BY player_id
            ORDER BY
                player_id, season
        ),0) lsispl,
        CASE
            WHEN LAG(pl) OVER(
                PARTITION BY player_id
                ORDER BY
                    player_id, season
            ) = 1 THEN
                LAG(games_minutes_played) OVER(
                    PARTITION BY player_id
                    ORDER BY
                        player_id, season
                )
            ELSE
                - 1
        END lsstats,
        t.*
    FROM
        q2_2 t
), q3 AS (
    SELECT
        *
    FROM
        q2_3
    WHERE
        league = 'Premier League'
        AND season <> s1
            AND team_id IN (
            SELECT
                team_id
            FROM
                tmp_premier_league_teams
        )
    ORDER BY
        to_number(player_id) ASC
)
SELECT
    t.*,
    p.position,
    p.nationality,
    to_number(substr(season, instr(t.season, '-') + 1, length(t.season))) - to_number(EXTRACT(YEAR FROM to_date(p.birth_date))) age
    ,
    p.height,
    p.weight,
    CASE
        WHEN t.league = l1 THEN
            0
        ELSE
            1
    END target1,
    CASE
        WHEN t.league = l1
             AND t.team_id <> t1 THEN
            1
        WHEN t.league = l1
             AND t.team_id = t1 THEN
            0
        ELSE
            - 1
    END target2
FROM
    q3       t,
    player   p
WHERE
    p.player_id = t.player_id
ORDER BY
    t.player_id;