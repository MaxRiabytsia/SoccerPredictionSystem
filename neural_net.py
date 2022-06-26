import pandas as pd
import numpy as np
from tensorflow import keras
from sklearn.model_selection import train_test_split
from constants import *


def get_numpy_dataset():
    df = pd.read_pickle("data/neural_net.pkl")
    df = df.drop("game_id", axis=1)

    # converting dataframe to numpy array
    dataset = df.to_numpy()
    tmp = np.ndarray(shape=(dataset.shape[0], NUM_OF_INPUTS + NUM_OF_OUTPUTS))
    for i, row in enumerate(dataset):
        tmp[i] = np.array(np.concatenate((row[0], row[1])))
    dataset = tmp

    return dataset


def train_test_data_labels_split(dataset: np.array):
    (train_dataset, test_dataset) = train_test_split(dataset, test_size=0.3)

    # splitting data from labels
    train_data = np.delete(train_dataset, np.s_[NUM_OF_INPUTS:NUM_OF_INPUTS + NUM_OF_OUTPUTS], axis=1)
    train_labels = np.delete(train_dataset, np.s_[:NUM_OF_INPUTS], axis=1)

    test_data = np.delete(test_dataset, np.s_[NUM_OF_INPUTS:NUM_OF_INPUTS + NUM_OF_OUTPUTS], axis=1)
    test_labels = np.delete(test_dataset, np.s_[:NUM_OF_INPUTS], axis=1)

    return (train_data, train_labels), (test_data, test_labels)


def create_model(train_data: np.array, train_labels: np.array):
    # we are using softmax in the output layer to convert a vector of output values into a vector of probabilities
    model = keras.Sequential([
        keras.layers.Dense(NUM_OF_INPUTS),  # input layer
        keras.layers.Dense(256, activation='sigmoid'),  # hidden layer
        keras.layers.Dense(3, activation='softmax')  # output layer
    ])

    model.compile(optimizer='Adam', loss='categorical_crossentropy', metrics=['accuracy'])

    model.fit(train_data, train_labels, epochs=10, batch_size=2048)

    return model


def main():
    dataset = get_numpy_dataset()

    (train_data, train_labels), (test_data, test_labels) = train_test_data_labels_split(dataset)

    model = create_model(train_data, train_labels)
    model.evaluate(test_data, test_labels)
    model.save("neural_net")


if __name__ == "__main__":
    main()
