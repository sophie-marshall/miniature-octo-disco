import json


def load_snowflake_connection_params(file_path: str):
    with open(file_path, "r") as file:
        snowflake_connection_params = json.load(file)
    return snowflake_connection_params
