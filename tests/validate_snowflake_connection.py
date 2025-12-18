from snowflake.snowpark import Session
import json

from utils.helpers import load_snowflake_connection_params


def build_session():
    """
    Build and return a Snowpark session using connection parameters from environment variables.
    """
    try:
        connection_params = load_snowflake_connection_params(
            "snowflake_connection.json"
        )

        session = Session.builder.configs(connection_params).create()

        return session

    except Exception as e:
        print("Failed to create Snowpark session")
        raise e


def test_session(session: Session):
    try:
        result = session.sql("SELECT CURRENT_TIMESTAMP()").collect()
        print("Connection test successful. Current timestamp:", result[0][0])
        return True
    except Exception as e:
        print(f"Connection test failed: {str(e)}")
        return False


if __name__ == "__main__":
    session = build_session()
    if test_session(session):
        print("Snowpark connection validated successfully.")
    else:
        print("Snowpark connection validation failed.")
