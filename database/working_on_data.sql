SELECT
    *
FROM
    player_trophies
WHERE
    player_id = 154
ORDER BY
    2,
    3,
    4;

SELECT
    *
FROM
    coach_trophies
WHERE
    coach_id = 1442
ORDER BY
    1,
    2,
    3,
    4;

SELECT
    *
FROM
    coaches;

SELECT
    *
FROM
    team;

SELECT
    *
FROM
    coaches
WHERE
    name LIKE '%Buruk%';

SELECT
    *
FROM
    countries;

SELECT
    *
FROM
    seasons;

SELECT
    *
FROM
    team;

SELECT
    *
FROM
    league;

SELECT
    *
FROM
    player
WHERE
    player_name <> 'Data not available'
    AND team_id = 91;

SELECT
    *
FROM
    player
WHERE
    player_name LIKE '%Seri%';

SELECT
    player_id,
    player_name,
    first_name,
    last_name,
    replace(convert(first_name, 'US7ASCII', 'AL32UTF8')
            || '%'
            || convert(last_name, 'US7ASCII', 'AL32UTF8'), ' ', '%')
FROM
    player
ORDER BY
    1;

SELECT
    *
FROM
    v$nls_valid_values
WHERE
    parameter = 'CHARACTERSET'
    AND value = 'TRUE';

SELECT
    player_id,
    replace(first_name, ' ', '%')
    || '%'
    || replace(last_name, ' ', '%')
FROM
    player;

SELECT
    *
FROM
    player_stats;

SELECT
    *
FROM
    all_tab_modifications;

SELECT
    player_id
FROM
    player
MINUS
SELECT
    player_id
FROM
    player_stats;

SELECT
    *
FROM
    player_stats
WHERE
    player_id = 874;

SELECT
    *
FROM
    coaches;

SELECT
    *
FROM
    coaches_career;

SELECT
    *
FROM
    coach_trophies;

TRUNCATE TABLE playertmarkt;

SELECT
    *
FROM
    playertmarkt
WHERE
    market_value = 'value';

DROP TABLE playertmarkt_2;

SELECT
    *
FROM
    playertmarkt_2
WHERE
    market_value = 'value';

SELECT
    player_id,
    player_name,
    first_name,
    last_name,
    replace(substr(replace(convert(first_name, 'US7ASCII', 'AL32UTF8'), '', ''), 1, instr(first_name, ''))
            || '%20'
            || convert(last_name, 'US7ASCII', 'AL32UTF8'), ' ', '%20') search,
    market_value
FROM
    playertmarkt
WHERE
    market_value = 'value'
ORDER BY
    1;

SELECT
    CASE
        WHEN rtrim(substr(first_name, 1, instr(first_name, ' '))) IS NULL THEN
            first_name
        ELSE
            rtrim(substr(first_name, 1, instr(first_name, ' ')))
    END AS quarter_cd
FROM
    playertmarkt;

SELECT
    player_id,
    player_name,
    first_name,
    last_name,
    replace(replace(convert(first_name, 'US7ASCII', 'AL32UTF8'), ' ', '%20')
            || '%20'
            || convert(last_name, 'US7ASCII', 'AL32UTF8'), ' ', '%20') search,
    market_value
FROM
    playertmarkt
WHERE
    market_value = 'value'
ORDER BY
    1;

SELECT
    player_id,
    player_name,
    first_name,
    last_name,
    position,
    t.name,
    round((sysdate - birth_date) / 365) - 1 age,
    nationality,
    convert(
        CASE
            WHEN rtrim(substr(first_name, 1, instr(first_name, ' '))) IS NULL THEN
                first_name
            ELSE
                rtrim(substr(first_name, 1, instr(first_name, ' ')))
        END
        || '%20'
        ||
        CASE
            WHEN rtrim(substr(last_name, 1, instr(last_name, ' '))) IS NULL THEN
                last_name
            ELSE
                rtrim(substr(last_name, 1, instr(last_name, ' ')))
        END, 'US7ASCII', 'AL32UTF8') search
FROM
    player   p,
    team     t
WHERE
    p.team_id = t.team_id
ORDER BY
    1;

SELECT
    *
FROM
    player;

SELECT
    player_id,
    first_name,
    last_name,
    t.name,
    round((sysdate - birth_date) / 365.25) age
FROM
    player   p,
    team     t
WHERE
    p.team_id = t.team_id
    AND last_name LIKE '%Muñoz%';

SELECT
    search1,
    search2,
    search3,
    search4,
    first_name,
    last_name,
    t.*
FROM
    playertmarkt3 t
WHERE
    ( market_value IS NULL )
    AND first_name <> 'None'
    AND last_name <> 'None';

SELECT
    market_value,
    COUNT(*) cnt
FROM
    playertmarkt3
WHERE
    ( market_value IS NULL
      OR market_value = '-' )
    AND first_name <> 'None'
    AND last_name <> 'None'
GROUP BY
    market_value;

SELECT
    player_id,
    player_name,
    first_name,
    last_name,
    position,
    team,
    age,
    nationality,
    search1,
    search2,
    search3,
    search4
FROM
    playertmarkt3
WHERE
    market_value IS NULL
    AND first_name <> 'None'
    AND last_name <> 'None';

SELECT
    *
FROM
    player
WHERE
    player_name LIKE '%Hazard%';

SELECT
    *
FROM
    playertmarkt3
ORDER BY
    player_id ASC;

SELECT
    *
FROM
    player
WHERE
    player_id = 129894;

SELECT
    player_name,
    birth_date,
    position
FROM
    player
ORDER BY
    player_id;

SELECT
    position,
    COUNT(*)
FROM
    player
GROUP BY
    position;

SELECT
    *
FROM
    player
WHERE
    player_name = 'Data not available';

SELECT
    trunc((trunc(sysdate) - trunc(birth_date)) / 365.25) age
FROM
    player
WHERE
    player_name LIKE '%Oelschlägel%'

update player set last_name = 'Lampropoulos' where player_id = 594;
commit;

select market_value,
replace(replace(replace(replace(market_value,'Th.','000'),'m','000000'),'.',''),'£','') from 
playertmarkt3 where market_value is not null and 
market_value <> '-';

select player_id,player_name,first_name,last_name,market_value,case when instr(replace(market_value,'£',''),'m') > 0 then
to_number(replace(replace(replace(market_value,'£',''),'.',','),'m','')) * 1000000
when instr(replace(market_value,'£',''),'Th.') > 0 then
to_number(replace(replace(market_value,'£',''),'Th.','')) * 1000 end mv
from playertmarkt3 where market_value is not null and 
market_value <> '-' order by 6 desc;

select to_number('2,1') from dual where instr('emrah.123','.123') > 0;

select * from player_stats where player_id = 641
order by season;

select * from player_stats where player_id in
(select distinct player_id from player_stats where league = 'Premier League')
order by 1;

select market_value,replace(replace(replace(market_value,'£',''),'.',','),'m',''),
replace(replace(replace(market_value,'£',''),'.',','),'Th.','') from playertmarkt3;

with q1 as
(select player_id,season,
lead(season,1) over (partition by player_id,league order by season asc) l1,
lead(season,2) over (partition by player_id,league order by season asc) l2,
lead(season,3) over (partition by player_id,league order by season asc) l3,
lead(season,4) over (partition by player_id,league order by season asc) l4,
lead(season,5) over (partition by player_id,league order by season asc) l5
from player_stats t where league = 'Premier League'
and (season in ('2015-2016','2016-2017','2017-2018','2018-2019','2019-2020'
,'2014-2015','2013-2014','2012-2013','2011-2012','2010-2011')))
select * from q1 where season is not null order by player_id; 

with q1 as
(select distinct player_id
from player_stats t where league = 'Premier League'
and (season in ('2015-2016','2016-2017','2017-2018','2018-2019','2019-2020'
,'2014-2015','2013-2014','2012-2013','2011-2012','2010-2011'
,'2009-2010','2008-2009','2007-2008')) and team_id in (select team_id from tmp_premier_league_teams)), q2 AS (
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
        player_stats ps,
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
    ORDER BY
        to_number(player_id),
        season
), q3 AS (
    SELECT
        *
    FROM
        q2
    WHERE
        league = 'Premier League'
        AND season <> s1
            AND l1 NOT IN (
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
            'Svenska Cupen'
        )
                AND team_id IN (
            SELECT
                team_id
            FROM
                tmp_premier_league_teams
        )
    ORDER BY
        to_number(player_id) ASC
)
select t.*,
         CASE
             WHEN league = l1 THEN
                 0
             ELSE
                 1
         END target1 from q3 t order by 1;
SELECT
    *
FROM
    team
WHERE
    name LIKE '%Sheffield Utd%';

SELECT
    *
FROM
    team
WHERE
    country IN (
        'England',
        'Wales'
    )
    AND name IN (
        'Hull City',
        'West Brom',
        'Stoke City',
        'Burnley',
        'Wolves',
        'Blackpool',
        'Swansea',
        'Norwich',
        'QPR',
        'Southampton',
        'Cardiff',
        'Crystal Palace',
        'Leicester',
        'Watford',
        'Bournemouth',
        'Brighton',
        'Huddersfield',
        'Sheffield Utd',
        'Manchester United',
        'Chelsea',
        'Arsenal',
        'Liverpool',
        'Everton',
        'Aston Villa',
        'Blackburn',
        'Portsmouth',
        'Manchester City',
        'West Ham',
        'Tottenham',
        'Middlesbrough',
        'Wigan',
        'Sunderland',
        'Bolton',
        'Fulham',
        'Reading',
        'Birmingham',
        'Derby',
        'Newcastle'
    )
ORDER BY
    2;

DESC player_stats;

SELECT
    *
FROM
    player_stats
WHERE
    team_id IN (
        SELECT
            team_id
        FROM
            tmp_premier_league_teams
    )
    AND league = 'Premier League'
        AND season IN (
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
    );