from flask import Flask
import pickle
import pandas as pd
from flask import jsonify
import json

app = Flask(__name__)


@app.route("/")
def index():
    return "Hello World!"


def dataframe():
    data = pickle.load(open("simple_wallet.pkl", "rb"))
    df = pd.DataFrame(data)
    return df


def json_method(dataframe):
    json = dataframe.to_json(orient="records")
    return json


@app.route("/api/v1/event-tracker")
def get_event_tracker():
    dataframe()
    df = dataframe().head(10)

    return json_method(df)


@app.route("/api/v1/event-tracker/<id>")
def get_event_tracker_by_id(id):
    df = dataframe()
    df = df[df["id"] == int(id)]

    return json_method(df)


@app.route("/api/v1/event-tracker/total-target")
def get_target_count():
    df = dataframe()
    target_count = df["target"].value_counts()
    target = target_count()

    return target.to_json()


def from_json(value):
    value_column = value
    value_column = [json.loads(user_str) for user_str in value_column]
    result = pd.DataFrame(value_column)

    return result


@app.route("/api/v1/event-tracker/total-user-type")
def get_user_type():
    df = dataframe()
    user_type = from_json(df["user"])

    type_counts = user_type["type"].value_counts()

    return type_counts.to_json()


@app.route("/api/v1/event-tracker/most-used")
def get_most_used():
    df = dataframe()
    user = from_json(df["user"])

    most_used = user.groupby(["id", "name"]).size().reset_index(name="count")
    sorted_most_used = most_used.sort_values(by="count", ascending=False).head(5)

    return sorted_most_used.to_json()


@app.route("/api/v1/event-tracker/common-used")
def get_common_used():
    df = dataframe()
    days_order = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]

    duration = df.loc[:, ["created_at"]]
    duration["created_at"] = pd.to_datetime(duration["created_at"])

    duration["day_of_week"] = duration["created_at"].dt.day_name()
    duration["day_of_week"] = pd.Categorical(
        duration["day_of_week"], categories=days_order, ordered=True
    )

    common_used = duration.groupby("day_of_week").size()

    return common_used.to_json()


if __name__ == "__main__":
    app.run(port=5000, debug=True)
