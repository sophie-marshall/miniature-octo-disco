from snowflake.snowpark import Session

from utils.helpers import load_snowflake_connection_params

# constants
POS_TABLES = [
    "country",
    "franchise",
    "location",
    "menu",
    "truck",
    "order_header",
    "order_detail",
]
CUSTOMER_TABLES = ["customer_loyalty"]
TABLE_DICT = {
    "pos": {"schema": "RAW_POS", "tables": POS_TABLES},
    "customer": {"schema": "RAW_CUSTOMER", "tables": CUSTOMER_TABLES},
}


def load_raw_table(session, tname=None, s3dir=None, year=None, schema=None):
    """
    Dynamically read Parquet files from external S3 stage.
    - Infer schema using speficied read option
    - Attach structured metadata to table for governance and lineage
    """
    # set our schema context -- equivilent to USE SCHEMA <schema>
    # this helps set a default space for our tables that are created
    session.use_role("HOL_ROLE")
    session.use_database("HOL_DB")
    session.use_schema(schema)

    # set s3 dir dynamically
    # offer support for date partitioned data and non-partitioned data
    # partitioning the data is useful and mimics what happens in warehouses anyways -- could benifit from partition pruning
    if year is None:
        location = f"@external.frostbyte_raw_stage/{s3dir}/{tname}"
    else:
        print("\tLoading year {}".format(year))
        location = f"@external.frostbyte_raw_stage/{s3dir}/{tname}/year={year}"

    # read parquet files from our stage, infer schema, apply snappy compression, and return a snowpark df
    df = session.read.option("compression", "snappy").parquet(location)

    # execute COPY INTO statement using the inferred schema from above
    df.copy_into_table(tname)

    # add metadata comment for filtering, ownership, etc
    comment_text = """{"origin":"sf_sit-is","name":"snowpark_101_de","version":{"major":1, "minor":0},"attributes":{"is_quickstart":1, "source":"sql"}}"""
    sql_command = f"""COMMENT ON TABLE {tname} IS '{comment_text}';"""
    session.sql(sql_command).collect()


def load_all_raw_tables(session):
    """
    Load all raw tables from external S3 into Snowflake using Snowpark
    - Adjust WH size for faster copy operations
    - Iterate over tables and schemas
    - Rever WH size on complete to save costs
    """
    print("Resizing warehouse for data load...")
    _ = session.sql(
        "ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE;"
    ).collect()

    for s3dir, data in TABLE_DICT.items():
        tnames = data["tables"]
        schema = data["schema"]
        for tname in tnames:
            print(f"Loading {tname}")
            if tname in ["order_header", "order_detail"]:
                for year in ["2019", "2020", "2021"]:
                    load_raw_table(
                        session, tname=tname, s3dir=s3dir, year=year, schema=schema
                    )
            else:
                load_raw_table(session, tname=tname, s3dir=s3dir, schema=schema)

    _ = session.sql(
        "ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XSMALL WAIT_FOR_COMPLETION = TRUE;"
    ).collect()


if __name__ == "__main__":
    # load connection params
    connection_params = load_snowflake_connection_params("snowflake_connection.json")

    # run script to load tables
    try:
        with Session.builder.configs(connection_params).create() as session:
            load_all_raw_tables(session)
    except Exception as e:
        print("Failed to create Snowpark session")
        raise e
