import pandas as pd
import numpy as np
from tensorflow import keras

MONEY = 1000


def make_bet(prediction: np.array, result: np.array, bet: float, odd: float):
    if prediction == np.argmax(result):
        return bet * odd - bet
    else:
        return -bet


def predict(model, game: np.array):
    probabilities = model(np.array([game]), training=False).numpy()
    prediction = np.argmax(probabilities)
    return probabilities[0][prediction], prediction


def determine_bet(confidence: float, min_prediction_confidence: float, min_bet: int, max_bet: int):
    bet_increase_per_confidence_percent = (max_bet - min_bet) / (100 - min_prediction_confidence * 100)

    if 0.7 <= confidence <= 1:
        bet_increase_per_confidence_percent *= 1.5

    bet = min_bet + bet_increase_per_confidence_percent * (confidence * 100 - min_prediction_confidence * 100)

    if bet < min_bet:
        return min_bet
    elif bet > max_bet:
        return max_bet

    return bet


def evaluate(model, df: pd.DataFrame, min_bet_limit: int, max_bet_limit: int, min_prediction_confidence: float,
             store_bets=False) -> pd.Series or None:
    if store_bets:
        bets = pd.DataFrame(columns=["game_id", "bet", "odd", "gain", "money"])
    money = MONEY
    bets_made = 0
    bets_won = 0
    biggest_win = 0
    biggest_loss = 0
    bet_sum = 0
    for i, game in df.iterrows():
        confidence, prediction = predict(model, game["data"])
        if confidence >= min_prediction_confidence:
            bet = determine_bet(confidence, min_prediction_confidence, min_bet_limit, max_bet_limit)
            if bet > money:
                bet = money * 0.75
            gain = make_bet(prediction, game["label"], bet, game["result_odd"])
            money += gain
            if store_bets:
                bets.loc[len(bets.index)] = pd.Series({"game_id": game.game_id, "bet": bet, "odd": game["result_odd"],
                                                       "gain": gain, "money": money})
            if money < 0:
                return None
            bet_sum += bet
            bets_made += 1
            if gain > biggest_win:
                biggest_win = gain
            elif gain < biggest_loss:
                biggest_loss = gain

            if gain > 0:
                bets_won += 1

    if bets_made == 0:
        return None

    data = {
        "min_bet_limit": min_bet_limit, "max_bet_limit": max_bet_limit,
        "min_prediction_confidence": min_prediction_confidence, "gain": money - MONEY,
        "biggest_win": biggest_win, "biggest_loss": biggest_loss, "average_bet": round(bet_sum / bets_made),
        "average_gain": (money - MONEY) / bets_made, "no_bets": round((df.shape[0] - bets_made) / df.shape[0], 3),
        "bets_won": round(bets_won / df.shape[0], 3), "bets_lost": round((bets_made - bets_won) / df.shape[0], 3)
    }
    eval_result = pd.Series(data=data, index=[
        "min_bet_limit", "max_bet_limit", "min_prediction_confidence", "gain", "biggest_win",
        "biggest_loss", "average_bet", "average_gain", "no_bets", "bets_won", "bets_lost"
    ])

    if store_bets:
        bets.to_pickle("data/best_params.pkl")

    return eval_result


def get_best_parameters(model, df: pd.DataFrame):
    eval_results = pd.DataFrame(columns=[
        "min_bet_limit", "max_bet_limit", "min_prediction_confidence", "gain", "biggest_win",
        "biggest_loss", "average_bet", "average_gain", "no_bets", "bets_won", "bets_lost"])

    for min_bet_limit in [10, 20, 50, 75]:
        for max_bet_limit in [50, 100, 200, 500]:
            if min_bet_limit > max_bet_limit:
                continue
            for min_prediction_confidence in [0.4, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8]:
                ser = evaluate(model, df, min_bet_limit, max_bet_limit, min_prediction_confidence)
                if ser is not None:
                    eval_results.loc[len(eval_results.index)] = ser

    eval_results.to_pickle("data/eval_results.pkl")

    return eval_results.sort_values("gain", ascending=False, ignore_index=True).loc[0]


def main():
    df = pd.read_pickle("data/neural_net_eval.pkl")

    # loading model saved in neural_net.py
    model = keras.models.load_model('neural_net')

    # getting the best betting parameters
    # min_bet_limit: min amount of money we can bet on a game
    # max_bet_limit: max amount of money we can bet on a game
    # min_prediction_confidence: min probability of the result that we are going to bet on
    best_params = get_best_parameters(model, df)

    # evaluating model with the best parameters and storing bets
    evaluate(model, df, best_params.min_bet_limit, best_params.max_bet_limit, best_params.min_prediction_confidence,
             True)


if __name__ == "__main__":
    main()
