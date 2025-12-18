from snowflake.snowpark import Session
import snowflake.snowpark.functions as F

from utils.logger import get_logger

from utils.helpers import load_snowflake_connection_params

logger = get_logger(__name__)


def create_pos_view(session: Session):
    """
    Create a flattened POS vidwew by joining multiple raw POS tables
    """
    # set schema to operate on
    session.use_schema("HARMONIZED")

    # make selections from the tables we'd like to join -- only select necessary columns
    order_detail = session.table("RAW_POS.ORDER_DETAIL").select(
        F.col("ORDER_DETAIL_ID"),
        F.col("LINE_NUMBER"),
        F.col("MENU_ITEM_ID"),
        F.col("QUANTITY"),
        F.col("UNIT_PRICE"),
        F.col("PRICE"),
        F.col("ORDER_ID"),
    )
    order_header = session.table("RAW_POS.ORDER_HEADER").select(
        F.col("ORDER_ID"),
        F.col("TRUCK_ID"),
        F.col("ORDER_TS"),
        F.to_date(F.col("ORDER_TS")).alias("ORDER_TS_DATE"),
        F.col("ORDER_AMOUNT"),
        F.col("ORDER_TAX_AMOUNT"),
        F.col("ORDER_DISCOUNT_AMOUNT"),
        F.col("LOCATION_ID"),
        F.col("ORDER_TOTAL"),
    )
    truck = session.table("RAW_POS.TRUCK").select(
        F.col("TRUCK_ID"),
        F.col("PRIMARY_CITY"),
        F.col("REGION"),
        F.col("COUNTRY"),
        F.col("FRANCHISE_FLAG"),
        F.col("FRANCHISE_ID"),
    )
    menu = session.table("RAW_POS.MENU").select(
        F.col("MENU_ITEM_ID"),
        F.col("TRUCK_BRAND_NAME"),
        F.col("MENU_TYPE"),
        F.col("MENU_ITEM_NAME"),
    )
    franchise = session.table("RAW_POS.FRANCHISE").select(
        F.col("FRANCHISE_ID"),
        F.col("FIRST_NAME").alias("FRANCHISEE_FIRST_NAME"),
        F.col("LAST_NAME").alias("FRANCHISEE_LAST_NAME"),
    )
    location = session.table("RAW_POS.LOCATION").select(F.col("LOCATION_ID"))

    # join truck with franchise on FRANCHISE_ID
    t_with_f = truck.join(
        franchise, truck["FRANCHISE_ID"] == franchise["FRANCHISE_ID"], rsuffix="_f"
    )

    # join order_header with truck+franchise on TRUCK_ID
    # then join location with the above on LOCATION_ID
    oh_w_t_and_l = order_header.join(
        t_with_f, order_header["TRUCK_ID"] == t_with_f["TRUCK_ID"], rsuffix="_t"
    ).join(
        location, order_header["LOCATION_ID"] == location["LOCATION_ID"], rsuffix="_l"
    )

    # join order_detail with the above using ORDER_ID
    # finally, join menu with the above using MENU_ITEM_ID
    final_df = order_detail.join(
        oh_w_t_and_l,
        order_detail["ORDER_ID"] == oh_w_t_and_l["ORDER_ID"],
        rsuffix="_oh",
    ).join(menu, order_detail["MENU_ITEM_ID"] == menu["MENU_ITEM_ID"], rsuffix="_m")

    # from our final joined df, select columns we want to create our flattened view
    final_df = final_df.select(
        F.col("ORDER_ID"),
        F.col("TRUCK_ID"),
        F.col("ORDER_TS"),
        F.col("ORDER_TS_DATE"),
        F.col("ORDER_DETAIL_ID"),
        F.col("LINE_NUMBER"),
        F.col("TRUCK_BRAND_NAME"),
        F.col("MENU_TYPE"),
        F.col("PRIMARY_CITY"),
        F.col("REGION"),
        F.col("COUNTRY"),
        F.col("FRANCHISE_FLAG"),
        F.col("FRANCHISE_ID"),
        F.col("FRANCHISEE_FIRST_NAME"),
        F.col("FRANCHISEE_LAST_NAME"),
        F.col("LOCATION_ID"),
        F.col("MENU_ITEM_ID"),
        F.col("MENU_ITEM_NAME"),
        F.col("QUANTITY"),
        F.col("UNIT_PRICE"),
        F.col("PRICE"),
        F.col("ORDER_AMOUNT"),
        F.col("ORDER_TAX_AMOUNT"),
        F.col("ORDER_DISCOUNT_AMOUNT"),
        F.col("ORDER_TOTAL"),
    )
    final_df.create_or_replace_view("POS_FLATTENED_V")


def create_pos_view_stream(session: Session):
    """
    Create a Snowflake STREAM object against the POS_FLATTENED_V view
    this allows us to track CDC across separate tables
    """

    session.use_schema("HARMONIZED")
    _ = session.sql(
        """
        CREATE OR REPLACE STREAM POS_FLATTENED_V_STREAM
        ON VIEW POS_FLATTENED_V
        SHOW_INITIAL_ROWS = TRUE        
        """
    ).collect()


def test_pos_view(session: Session):
    session.use_schema("HARMONIZED")
    tv = session.table("POS_FLATTENED_V")
    tv.limit(5).show()


if __name__ == "__main__":

    try:
        logger.debug("Loading Snowflake connection params")
        # load connection params
        connection_params = load_snowflake_connection_params(
            "snowflake_connection.json"
        )

    except Exception as e:
        logger.error(f"Error loading Snowflake connection params: {e}")
        raise

    with Session.builder.configs(connection_params).create() as session:
        logger.debug("Snowflake session created successfully")

        try:
            logger.debug("Creating POS flattened view and stream")
            create_pos_view(session)
            create_pos_view_stream(session)

            logger.debug("Testing POS flattened view")
            test_pos_view(session)

            logger.info("POS flattened view and stream created and tested successfully")

        except Exception as e:
            logger.error(f"Error during POS view creation or testing: {e}")
            raise
