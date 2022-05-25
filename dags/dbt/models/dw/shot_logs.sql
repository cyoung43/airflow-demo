
select
    nba.GAME_ID as GAME_ID,
    nba.MATCHUP as MATCHUP,
    nba.LOCATION as LOCATION,
    nba.player_name as PLAYER_NAME,
    nba.player_id::varchar as PLAYER_ID,
    nba.SHOT_NUMBER as SHOT_NUMBER,
    nba.PERIOD as QUARTER,
    nba.GAME_CLOCK as GAME_CLOCK,
    nba.SHOT_CLOCK as SHOT_CLOCK,
    nba.DRIBBLES as DRIBBLES,
    nba.TOUCH_TIME as TOUCH_TIME,
    nba.SHOT_DIST as SHOT_DISTANCE,
    nba.PTS_TYPE as POINTS_TYPE,
    nba.SHOT_RESULT as SHOT_RESULT,
    nba.CLOSEST_DEFENDER as CLOSEST_DEFENDER,
    nba.CLOSEST_DEFENDER_PLAYER_ID::varchar as DEFENDER_ID,
    nba.CLOSE_DEF_DIST as DEFENDER_DISTANCE,
    nba.FGM as FIELD_GOAL_MADE,
    nba.PTS as POINTS,
    nba.W as WIN,
    nba.FINAL_MARGIN as FINAL_MARGIN
from {{ source('airbyte', 'NBA_DATA') }} nba