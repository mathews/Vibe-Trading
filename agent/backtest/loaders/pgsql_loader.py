import logging
import os

import pandas as pd
from sqlalchemy import URL, create_engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.sql import text

from backtest.loaders.registry import register

engine = None
QUANT_PG_DATA_SQL = None

logger = logging.getLogger(__name__)


def __init__():
    global engine
    global QUANT_PG_DATA_SQL

    QUANT_PG_USER_NAME = os.getenv("QUANT_PG_USER_NAME")
    QUANT_PG_USER_PASSWD = os.getenv("QUANT_PG_USER_PASSWD")
    QUANT_PG_SERVER_HOST = os.getenv("QUANT_PG_SERVER_HOST")
    QUANT_PG_DB_NAME = os.getenv("QUANT_PG_DB_NAME")
    QUANT_PG_DATA_SQL = os.getenv("QUANT_PG_DATA_SQL")

    _url_object = URL.create(
        "postgresql+psycopg2",
        username=QUANT_PG_USER_NAME,
        # password=db_passwd,  # plain (unescaped) text
        password=QUANT_PG_USER_PASSWD,
        host=QUANT_PG_SERVER_HOST,
        database=QUANT_PG_DB_NAME,
    )

    engine = create_engine(
        _url_object,
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
    )


@register
class DataLoader:
    name = "pg_source"  # the value you pass as source=
    markets = {
        "us_equity",
        "a_share",
        "hk_equity",
        "crypto",
        "futures",
        "fund",
        "macro",
        "forex",
    }  # a_share/us_equity/hk_equity/crypto/futures/fund/macro/forex
    requires_auth = True

    def is_available(self) -> bool:
        if engine is None:
            __init__()
        if engine is None:
            return False
        return True  # token present? network reachable?

    def fetch(
        self,
        codes: list[str] | None,
        start_date: str,
        end_date: str,
        *,
        interval: str = "1D",
        fields: list[str] | None = None,
    ):
        # return {symbol: DataFrame indexed by trade_date,
        #         columns: open, high, low, close, volume}
        if engine is None:
            __init__()

        if engine is None or QUANT_PG_DATA_SQL is None:
            raise RuntimeError("Database engine is not initialized.")

        # if fields is not None:
        #     fields_str = ", ".join(fields)
        # else:
        #     fields_str = "open, high, low, close, volume"

        # fields_str = "symbol, ts_code, trade_date, " + fields_str

        with engine.connect() as conn:
            # sql = text(
            #     """
            #     select symbol, ts_code, trade_date,  open, high, low, close, volume from :table_name
            #     where trade_date >= :start_time and trade_date <= :end_time
            #     and symbol in :symbols
            #     """,
            # )
            if codes is not None:
                sql = text(
                    QUANT_PG_DATA_SQL + " and symbol in :symbols"
                ).bindparams(symbols=tuple(codes))
            else:
                sql = text(QUANT_PG_DATA_SQL)

            result = conn.execute(
                sql,
                {
                    "end_time": end_date,
                    "start_time": start_date,
                },
            ).fetchall()

            logger.info(f"需要更新的股票数量: {len(result)}")

            ret_df = pd.DataFrame(
                result,
                columns=[
                    "symbol",
                    "trade_date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                ],
            )
            return ret_df
