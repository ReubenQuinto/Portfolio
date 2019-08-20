from flask import Flask, render_template, jsonify, request
import pandas as pd
import numpy as np
import random

from models.monteCarlo import *


app = Flask(__name__)

# home page
@app.route("/")
def home():
    return render_template("index.html")

@app.route("/data", methods=["POST"])
def convert():

    # user input
    userData_numSimulations = request.form['user_input_1']
    userData_stock = request.form['user_input_2']

    # instantiate
    stock = {
        "^GSPC":"S&P 500",
        "TSLA": "Tesla",
        "GOOG": "Google",
        "JPM": "JP Morgan Chase",
        "TM": "Toyota"
    }

    url_file = "./data/{}.csv".format(userData_stock)

    df = pd.read_csv(url_file)
    df = df
    df = df['Adj Close'].diff()
    df = df[1:len(df)]

    avg = np.average(df)
    std = np.std(df)

    number_of_samples = int(userData_numSimulations)
    monteCarlo = monteCarlo_simulation(number_of_samples, avg, std)

    # create responses
    output_distribution = monteCarlo_distribution(monteCarlo)
    output_sorted_list = monteCarlo_list(monteCarlo)
    response = [{
        "distribution": output_distribution,
        "sorted_list": output_sorted_list,
        "stock": userData_stock
    }]

    return jsonify({"success": True, "output": response})

# Permit: Command line execution
if __name__ == "__main__":
    app.run(debug=True)