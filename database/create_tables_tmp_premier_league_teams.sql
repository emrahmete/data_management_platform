CREATE TABLE tmp_premier_league_teams
    AS
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