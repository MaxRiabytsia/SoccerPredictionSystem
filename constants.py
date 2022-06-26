HEADERS = {'x-rapidapi-key': 'xxxxxxxxxxxxxxxxxxxxxxxxxxxx', 'x-rapidapi-host': 'v3.football.api-sports.io'}
BASE_URL = "https://v3.football.api-sports.io"

DB_NAME = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
HOST = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.rds.amazonaws.com"
PORT = "xxxx"
USERNAME = "xxxxxxxxxxxxxxxx"
PASSWORD = "xxxxxxxxxxxxxxxxxxxx"


FIRST_SEASON = 2011
LAST_SEASON = 2021
LEAGUES1 = [39, 78, 140, 61, 135, 88, 144, 94]
LEAGUES2 = [554, 71, 219, 40, 207, 120, 197, 597]
LEAGUES3 = [333, 271, 218, 345, 210, 79, 62, 141, 113, 136, 103, 244, 119, 179, 357, 110, 106, 203, 169, 253]

EXTRA_EVALUATION_LEAGUES = [39, 78, 135, 61, 140, 88, 144, 94, 79, 71, 219, 40, 207, 62, 197, 141, 271, 136, 345, 210]

NUM_OF_LAST_GAMES = 15
NUM_OF_LAST_HEAD2HEADS = 4
MIN_NUM_OF_LAST_GAMES = 15
MIN_NUM_OF_LAST_HEAD2HEADS = 4
MAX_DAYS_SINCE_GAME = 365
MAX_DAYS_SINCE_HEAD2HEAD = 365 * 3

NUM_OF_OUTPUTS = 3
NUM_OF_INPUTS = 2 * NUM_OF_LAST_GAMES + NUM_OF_LAST_HEAD2HEADS

BOOKMAKER = 1
