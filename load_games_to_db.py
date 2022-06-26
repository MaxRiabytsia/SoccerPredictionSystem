import pymysql
import pandas as pd
from constants import *


def connect_to_db():
    connection = pymysql.connect(host=HOST, user=USERNAME, password=PASSWORD)

    if connection:
        print('Connected!')
        return connection
    else:
        raise Exception("Something went wrong...")


def create_games_table(cursor):
    create_table_command = ("CREATE TABLE IF NOT EXISTS games (\n"
                            "		game_id INT NOT NULL PRIMARY KEY,\n"
                            "		home_team_id INT NOT NULL,\n"
                            "		away_team_id INT NOT NULL,\n"
                            "		result INT NOT NULL,\n"
                            "		goal_difference INT NOT NULL,\n"
                            "		date DATE NOT NULL,\n"
                            "		league_id INT NOT NULL,\n"
                            "		season INT NOT NULL\n"
                            "	)")
    cursor.execute(create_table_command)
    cursor.connection.commit()


def insert_into_games_table(cursor, game_id, home_team_id, away_team_id, result,
                            goal_difference, date, league_id, season):
    insert_row_command = (
        "INSERT IGNORE INTO games (game_id, home_team_id, away_team_id, result, goal_difference, date, league_id, "
        "season) \n "
        "		VALUES (%s,%s,%s,%s,%s,%s,%s,%s)")
    row_to_insert = (game_id, home_team_id, away_team_id, result, goal_difference, date, league_id, season)
    cursor.execute(insert_row_command, row_to_insert)
    cursor.connection.commit()


def append_from_df_to_games_db(cursor, df):
    for i, row in df.iterrows():
        insert_into_games_table(cursor, row['game_id'], row['home_team_id'], row['away_team_id'], row['result'],
                                row['goal_difference'], row['date'], row['league_id'], row['season'])


def fill_games_db(cursor):
    df = pd.read_pickle("data/games.pkl")

    cursor.execute("USE football_prediction_db")
    create_games_table(cursor)

    append_from_df_to_games_db(cursor, df)


def create_eval_table(cursor):
    create_table_command = ("CREATE TABLE IF NOT EXISTS evaluation (\n"
                            "		game_id INT NOT NULL PRIMARY KEY,\n"
                            "		home_team_id INT NOT NULL,\n"
                            "		away_team_id INT NOT NULL,\n"
                            "		result INT NOT NULL,\n"
                            "		goal_difference INT NOT NULL,\n"
                            "		date DATE NOT NULL,\n"
                            "		league_id INT NOT NULL,\n"
                            "		season INT NOT NULL,\n"
                            "       home_odd FLOAT NOT NULL,\n"
                            "       draw_odd FLOAT NOT NULL,\n"
                            "       away_odd FLOAT NOT NULL\n"
                            "	)")
    cursor.execute(create_table_command)
    cursor.connection.commit()


def insert_into_eval_table(cursor, game_id, home_team_id, away_team_id, result, goal_difference,
                           date, league_id, season, home_odd, draw_odd, away_odd):
    insert_row_command = (
        "INSERT IGNORE INTO evaluation ("
        "game_id, home_team_id, away_team_id, result, goal_difference,"
        "date, league_id, season, home_odd, draw_odd, away_odd) \n"
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
    row_to_insert = (game_id, home_team_id, away_team_id, result, goal_difference,
                     date, league_id, season, home_odd, draw_odd, away_odd)
    cursor.execute(insert_row_command, row_to_insert)
    cursor.connection.commit()


def append_from_df_to_eval_db(cursor, df):
    for i, row in df.iterrows():
        insert_into_eval_table(cursor, row['game_id'], row['home_team_id'], row['away_team_id'],
                               row['result'], row['goal_difference'], row['date'], row['league_id'],
                               row['season'], row['home_odd'], row['draw_odd'], row['away_odd'])


def fill_eval_db(cursor):
    df = pd.read_pickle("data/evaluation.pkl")

    cursor.execute("USE football_prediction_db")
    create_eval_table(cursor)

    append_from_df_to_eval_db(cursor, df)


def main():
    # connecting to database
    connection = connect_to_db()
    cursor = connection.cursor()

    # creating tables and filling them in
    fill_games_db(cursor)
    fill_eval_db(cursor)


if __name__ == "__main__":
    main()
