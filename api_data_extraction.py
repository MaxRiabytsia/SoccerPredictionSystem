import requests
import pandas as pd
import json
import time
from datetime import datetime
from constants import *


def days_from_to(from_date: str, to_date: str) -> int:
	from_date = datetime.strptime(from_date, "%Y-%m-%d")
	to_date = datetime.strptime(to_date, "%Y-%m-%d")
	return (to_date - from_date).days


def get_winner(game: dict) -> int:
	if game["teams"]["home"]["winner"]:
		return 1
	elif game["teams"]["away"]["winner"]:
		return -1
	else:
		return 0


def all_league_season_games(league_id: int, season: int) -> pd.DataFrame:
	# API call
	url = f"{BASE_URL}/fixtures?league={league_id}&season={season}"
	response = requests.request("GET", url, headers=HEADERS, data={})
	parsed = json.loads(response.text)

	# creating and filling in the DataFrame
	df = pd.DataFrame(columns=[
		"game_id", "home_team_id", "away_team_id", "goal_difference", "result", "date", "league_id", "season"])
	for game in parsed["response"]:
		if game["fixture"]["status"]["long"] == "Match Finished":
			df.loc[df.shape[0]] = pd.Series({
				"game_id": game["fixture"]["id"],
				"home_team_id": game["teams"]["home"]["id"],
				"away_team_id": game["teams"]["away"]["id"],
				"goal_difference": game["goals"]["home"] - game["goals"]["away"],
				"result": get_winner(game),
				"date": game["fixture"]["date"][:10:],
				"league_id": league_id,
				"season": season
			})

	return df


def get_odds_df(league_id: int, api_calls: int) -> pd.DataFrame:
	odds_df_list = []
	page_number = 1
	# we are iterating through all pages of games with odds
	while True:
		# API call
		url = f"{BASE_URL}/odds?league={league_id}&season={LAST_SEASON}&bookmaker={BOOKMAKER}&bet=1&page={page_number}"
		response = requests.request("GET", url, headers=HEADERS, data={})
		parsed = json.loads(response.text)
		api_calls = check_api_call_limit(api_calls)

		# creating and filling in the DataFrame
		odds_df = pd.DataFrame(columns=["game_id", "home_odd", "draw_odd", "away_odd"])
		for game in parsed["response"]:
			# making sure we get only finished games
			if days_from_to(datetime.now().strftime("%Y-%m-%d"), game["fixture"]["date"][:10:]) < 0:
				# sometimes API does not have full information (e.g. away_odd missing)
				# this block catches it
				try:
					odds_df.loc[odds_df.shape[0]] = pd.Series({
						"game_id": game["fixture"]["id"],
						"home_odd": game["bookmakers"][0]["bets"][0]["values"][0]["odd"],
						"draw_odd": game["bookmakers"][0]["bets"][0]["values"][1]["odd"],
						"away_odd": game["bookmakers"][0]["bets"][0]["values"][2]["odd"],
					})
				except IndexError:
					continue

		odds_df_list.append(odds_df)
		if page_number == parsed["paging"]["total"]:
			break
		else:
			page_number += 1

	if len(odds_df_list) != 0:
		return pd.concat(odds_df_list, ignore_index=True, sort=False)
	else:
		return pd.DataFrame(columns=["game_id", "home_odd", "draw_odd", "away_odd"])


def check_api_call_limit(api_calls):
	# API has restriction on amount of requests (10 per minute)
	# so we call time.sleep(61) every 10 calls
	api_calls += 1
	if api_calls == 10:
		time.sleep(61)
		return 0
	else:
		return api_calls


def main():
	data_frames = []
	evaluation_dfs = []
	dict_last_season_games = {}
	api_calls = 0

	for league in LEAGUES1 + LEAGUES2 + LEAGUES3:
		if league not in EXTRA_EVALUATION_LEAGUES:
			# API stores odds only for one week, therefore we are looking for the games with odds only in last season
			last_season_games = all_league_season_games(league, LAST_SEASON)
			api_calls = check_api_call_limit(api_calls)
			evaluation_dfs.append(pd.merge(last_season_games, get_odds_df(league, api_calls), on="game_id"))
			# to avoid making the same request twice we store DataFrames for LAST_SEASON in a dictionary
			dict_last_season_games[league] = last_season_games

		for season in range(FIRST_SEASON, LAST_SEASON+1):
			if league in EXTRA_EVALUATION_LEAGUES and season != FIRST_SEASON:
				continue
			if season == LAST_SEASON:
				data_frames.append(dict_last_season_games[league])
			else:
				data_frames.append(all_league_season_games(league, season))
				api_calls = check_api_call_limit(api_calls)

	# creating final DataFrames and save them in .pkl files
	if len(data_frames) != 0:
		df = pd.concat(data_frames, ignore_index=True, sort=False)
		if len(evaluation_dfs) != 0:
			evaluation_df = pd.concat(evaluation_dfs, ignore_index=True, sort=False)
			evaluation_df.to_pickle("data/evaluation.pkl")
			# deleting evaluation games from games dataset
			df = df.loc[df.index.difference(evaluation_df.index, sort=False)]
		df.to_pickle("data/games.pkl")


if __name__ == "__main__":
	main()
