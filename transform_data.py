import pandas as pd
import numpy as np
import pymysql
from datetime import datetime
from constants import *


def days_from_to(from_date: datetime, to_date: datetime) -> int:
    return (to_date - from_date).days


def get_odd_name(result: int) -> str:
    # returning odd name based on the result
    if result == 1:
        return "home_odd"
    elif result == 0:
        return "draw_odd"
    else:
        return "away_odd"


def get_label(res: int) -> np.array:
    # returning label based on the result
    if res == 1:
        return np.array([1, 0, 0])
    elif res == 0:
        return np.array([0, 1, 0])
    else:
        return np.array([0, 0, 1])


def check_games_results(games_results: list, minimum: int, maximum: int) -> list:
    # if length of games_results is equal to what we want it to be, we return it
    if len(games_results) == maximum:
        return games_results
    # if length of games_results is bigger than minimum length, we generate remaining games result
    # note: if min == max, then the generating is not happening, and we work with what we have
    elif len(games_results) >= minimum:
        if len(games_results) == 0:
            avg = 0
        else:
            avg = round(sum(games_results) / len(games_results))
        for i in range(maximum - len(games_results)):
            games_results.append(avg)
    # if length of games_results is less than minimum length, we return None
    # which will later lead to not using this game at all
    else:
        games_results = None

    return games_results


def n_last_team_games(df: pd.DataFrame, team_id: int, league_id: int, n: int, prediction_game_date: datetime):
    games_results = []
    # filtering out not relevant information
    df0 = df.loc[(df['league_id'] == league_id) & ((df['home_team_id'] == team_id) | (df['away_team_id'] == team_id))]
    # we iterate in reverse order so that we include the latest games
    for i in range(df0.shape[0] - 1, -1, -1):
        game = df0.iloc[i]
        # we don't want to include games that did not happen yet when this game took place,
        # and also we don't want to use the games that happened more than 'MAX_DAYS_SINCE_GAME' days ago,
        # this can happen because a team can leave a league for a few seasons and then come back,
        # but we don't want to use the results from years ago since they are not relevant anymore
        if 0 < days_from_to(game["date"], prediction_game_date) < MAX_DAYS_SINCE_GAME:
            if game["home_team_id"] == team_id:
                games_results.append(game["goal_difference"])
            elif game["away_team_id"] == team_id:
                games_results.append(game["goal_difference"] * -1)

            if len(games_results) == n:
                break

    games_results = check_games_results(games_results, MIN_NUM_OF_LAST_GAMES, NUM_OF_LAST_GAMES)

    return games_results


def n_last_head2head(df: pd.DataFrame, home_team_id: int, away_team_id: int, n: int, prediction_game_date: datetime):
    games_results = []
    # filtering out not relevant information
    df0 = df.loc[((df['home_team_id'] == home_team_id) & (df['away_team_id'] == away_team_id)) |
                 ((df['home_team_id'] == away_team_id) & (df['away_team_id'] == home_team_id))]
    # we iterate in reverse order so that we include the latest games
    for i in range(df0.shape[0] - 1, -1, -1):
        game = df0.iloc[i]
        # we don't want to include games that did not happen when this game took place,
        # and also we don't want to use the games that happened more than 'MAX_DAYS_SINCE_HEAD2HEAD' days ago,
        # this can happen because a team can leave a league for a few seasons and then come back,
        # but we don't want to use the results from years ago since they are not relevant anymore
        if 0 < days_from_to(game["date"], prediction_game_date) < MAX_DAYS_SINCE_HEAD2HEAD:
            if game["home_team_id"] == home_team_id:
                games_results.append(game["goal_difference"])
            else:
                games_results.append(game["goal_difference"] * -1)

        if len(games_results) == n:
            return games_results

    games_results = check_games_results(games_results, MIN_NUM_OF_LAST_HEAD2HEADS, NUM_OF_LAST_HEAD2HEADS)

    return games_results


def get_and_merge_all_data(df: pd.DataFrame, row: pd.Series):
    head2head = n_last_head2head(
        df, row.loc["home_team_id"], row.loc["away_team_id"], NUM_OF_LAST_HEAD2HEADS, row.loc["date"])
    if head2head is None:
        return None

    last_results_home_team = n_last_team_games(
        df, row.loc["home_team_id"], row.loc["league_id"], NUM_OF_LAST_GAMES, row.loc["date"])
    if last_results_home_team is None:
        return None

    last_results_away_team = n_last_team_games(
        df, row.loc["away_team_id"], row.loc["league_id"], NUM_OF_LAST_GAMES, row.loc["date"])
    if last_results_away_team is None:
        return None

    # merging all data in a numpy array
    data = np.array(head2head + last_results_home_team + last_results_away_team)

    return data


def get_training_testing_df(games_df: pd.DataFrame):
    neural_net_df = pd.DataFrame(columns=["game_id", "data", "label"])
    for i, row in games_df.iterrows():
        data = get_and_merge_all_data(games_df, row)
        # data can be None if we don't have enough data to fill in some components (head2heads, last_n_games, etc)
        if data is None:
            continue

        neural_net_df.loc[neural_net_df.shape[0]] = pd.Series({
            "game_id": row.game_id,
            "data": data,
            "label": get_label(row.loc["result"])
        })

    return neural_net_df


def get_evaluation_df(games_df: pd.DataFrame, eval_df: pd.DataFrame):
    neural_net_eval_df = pd.DataFrame(columns=["game_id", "data", "label", "result_odd"])
    for i, row in eval_df.iterrows():
        data = get_and_merge_all_data(games_df, row)
        # data can be None if we don't have enough data to fill in some components (head2heads, last_n_games, etc)
        if data is None:
            continue

        neural_net_eval_df.loc[len(neural_net_eval_df.index)] = pd.Series({
            "game_id": row["game_id"],
            "data": data,
            "label": get_label(row.loc["result"]),
            "result_odd": np.float64(row.loc[get_odd_name(row.loc["result"])])
        })

    return neural_net_eval_df


def connect_to_db():
    connection = pymysql.connect(host=HOST, user=USERNAME, password=PASSWORD)

    if connection:
        print('Connected!')
        return connection
    else:
        raise Exception("Something went wrong...")


def main():
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute("USE football_prediction_db")

    games_df = pd.read_sql("SELECT * FROM games", connection)
    eval_df = pd.read_sql("SELECT * FROM evaluation", connection)

    # Getting dfs for neural network
    neural_net_df = get_training_testing_df(games_df)
    neural_net_df.to_pickle("data/neural_net.pkl")
    neural_net_eval_df = get_evaluation_df(games_df, eval_df)
    neural_net_eval_df.to_pickle("data/neural_net_eval.pkl")


if __name__ == "__main__":
    main()
