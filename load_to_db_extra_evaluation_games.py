import pandas as pd
import requests
import json
import time

from constants import *
from load_games_to_db import append_from_df_to_eval_db, connect_to_db


def get_games_df(connection):
    games_df = pd.read_sql("SELECT * FROM games", connection)

    # filtering out information about seasons other than 2015, 2016, 2017, 2018
    games_df = games_df[(games_df.season >= 2015) & (games_df.season <= 2018)]

    return games_df


def get_odds_df():
    # loading extra games with odds because API stores odds only for one week
    odds_df = pd.read_csv("data/odds/Matches_Odds.csv")

    # dropping not relevant information
    odds_df = odds_df.drop_duplicates("match_id")
    odds_df = odds_df.drop(columns=["date_created"])

    # converting date format to correspond to the one we use
    odds_df.date_start = pd.to_datetime(odds_df.date_start).dt.date

    return odds_df


def get_team_name_id_dict():
    name_id_dict = {}
    api_calls = 0  # variable that keeps track of number of API calls made (we can only make 10 per minute)
    for league in LEAGUES1 + LEAGUES2 + LEAGUES3:
        # extra evaluation file has only 2015, 2016, 2017, 2018 seasons
        for season in [2015, 2016, 2017, 2018]:
            # API call
            url = f"{BASE_URL}/teams?league={league}&season={season}"
            response = requests.request("GET", url, headers=HEADERS, data={})
            parsed = json.loads(response.text)
            api_calls += 1
            if api_calls == 10:
                time.sleep(61)
                api_calls = 0

            for team in parsed["response"]:
                if team["team"]["name"] not in name_id_dict:
                    name_id_dict[team["team"]["name"]] = team["team"]["id"]

    return name_id_dict


def get_extra_evaluation_games(games_df: pd.DataFrame, odds_df: pd.DataFrame):
    name_id_dict = get_team_name_id_dict()

    eval_df = pd.DataFrame(columns=["game_id", "home_team_id", "away_team_id", "goal_difference", "result", "date",
                                    "league_id", "season", "home_odd", "draw_odd", "away_odd"])

    for i, game in odds_df.iterrows():
        # finding the game from odds_df in games_df and storing it in no_odds_game
        if game.home_team_name in name_id_dict and game.away_team_name in name_id_dict:
            no_odds_game = games_df.loc[(games_df.home_team_id == name_id_dict[game.home_team_name]) &
                                        (games_df.away_team_id == name_id_dict[game.away_team_name]) &
                                        (games_df.date == game.date_start)]
            if no_odds_game.shape[0] != 0:
                no_odds_game = no_odds_game.iloc[0]
            else:
                continue
        else:
            continue

        eval_df.loc[eval_df.shape[0]] = pd.Series({
            "game_id": no_odds_game.game_id,
            "home_team_id": no_odds_game.home_team_id,
            "away_team_id": no_odds_game.away_team_id,
            "goal_difference": no_odds_game.goal_difference,
            "result": no_odds_game.result,
            "date": no_odds_game.date,
            "league_id": no_odds_game.league_id,
            "season": no_odds_game.season,
            "home_odd": game.home_team_odd,
            "draw_odd": game.tie_odd,
            "away_odd": game.away_team_odd,
        })

    eval_df = eval_df.drop_duplicates(ignore_index=True)
    return eval_df


def main():
    # connect to database
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute("USE football_prediction_db")

    # get the dataframes
    games_df = get_games_df(connection)
    odds_df = get_odds_df()
    eval_df = get_extra_evaluation_games(games_df, odds_df)

    # delete new evaluation games from games db
    for i, game in eval_df.iterrows():
        cursor.execute(f"DELETE FROM games WHERE game_id = {game.game_id}")

    # load new evaluation games to db
    append_from_df_to_eval_db(cursor, eval_df)


if __name__ == "__main__":
    main()
