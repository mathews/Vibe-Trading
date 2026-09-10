import logging
import os

import pandas as pd
from sqlalchemy import URL, create_engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.sql import text

from backtest.loaders.registry import register
from src.tools.alpha_bench_tool import _CSI300_FALLBACK_CODES, _parse_period

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
                sql = text(QUANT_PG_DATA_SQL + " and symbol in :symbols").bindparams(
                    symbols=tuple(codes)
                )
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


def _pg_load_universe(
    universe: str,
    period: str,
) -> dict[str, pd.DataFrame]:  # pyright: ignore[reportUnusedFunction]
    global engine

    if engine is None:
        __init__()
    if engine is None:
        return {}
    codes = list(_CSI300_FALLBACK_CODES)

    start, end = _parse_period(period)

    with engine.connect() as conn:
        sql = text(QUANT_PG_UNIVERSE_SQL)
        result = conn.execute(sql).fetchall()
        return [row[0] for row in result]


def _load_csi300_panel(start: str, end: str) -> dict[str, pd.DataFrame]:
    """CSI 300 panel via Tushare. Includes ``amount`` (required by gtja191).

    Constituents are taken from the most recent ``index_weight`` snapshot in
    the requested window; if that call fails we degrade to a 30-name
    blue-chip fallback so the bench still runs.
    """
    token = get_env_config().data.tushare_token.strip()
    if not token or token == "your-tushare-token":
        raise RuntimeError(
            "TUSHARE_TOKEN not in agent/.env or environment; required for csi300 universe"
        )

    try:
        import tushare as ts
    except ImportError as exc:
        raise RuntimeError(f"tushare not installed: {exc}") from exc

    pro = ts.pro_api(token)
    sd = start.replace("-", "")
    ed = end.replace("-", "")

    codes: list[str] = []
    constituent_source = "tushare index_weight"
    constituent_source_date: str | None = None
    membership: pd.DataFrame | None = None
    try:
        # Reach back before ``start`` so the snapshot that was in force on the
        # first requested day is included; Tushare publishes month-end rosters.
        lookback = (pd.Timestamp(start) - pd.Timedelta(days=60)).strftime("%Y%m%d")
        weights = pro.index_weight(index_code="399300.SZ", start_date=lookback, end_date=ed)
        if weights is not None and not weights.empty:
            frame = weights.copy()
            frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
            frame = frame.dropna(subset=["trade_date", "con_code"])
            constituent_source_date = str(weights["trade_date"].max())
            # Every name that was a member at any point in the window, so the
            # panel can carry a name that later left the index.
            codes = sorted(frame["con_code"].astype(str).unique())
            membership = (
                frame.assign(_member=True)
                .pivot_table(
                    index="trade_date",
                    columns="con_code",
                    values="_member",
                    aggfunc="first",
                )
                .notna()
                .sort_index()
            )
            logger.info(
                "csi300: %d names ever a member across %d roster snapshots",
                len(codes),
                len(membership),
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning("csi300 index_weight failed (%s); using fallback list", exc)

    if not codes:
        codes = list(_CSI300_FALLBACK_CODES)
        constituent_source = "hand-picked fallback"
        constituent_source_date = None
        logger.warning("csi300: using %d-name fallback (degraded run)", len(codes))

    # Fetch raw daily in parallel — we need ``amount`` which the standard
    # loader drops. Tushare's free tier permits ~200 calls/min so 4 concurrent
    # workers is comfortably under the rate limit even for a full 300-name list.
    def _fetch_one(code: str) -> tuple[str, pd.DataFrame | None]:
        df = _retry(lambda: pro.daily(ts_code=code, start_date=sd, end_date=ed))
        if df is None or df.empty:
            return code, None
        df = df.sort_values("trade_date").copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.set_index("trade_date")
        df = df.rename(columns={"vol": "volume"})
        for col in ("open", "high", "low", "close", "volume", "amount"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        keep = [c for c in ("open", "high", "low", "close", "volume", "amount") if c in df.columns]
        df = df[keep].dropna(subset=["open", "high", "low", "close"])
        factor = _retry(lambda: pro.adj_factor(ts_code=code, start_date=sd, end_date=ed))
        return code, _apply_qfq(df, factor)

    fetched: dict[str, pd.DataFrame] = {}
    with ThreadPoolExecutor(max_workers=_CSI300_FETCH_WORKERS) as pool:
        futures = [pool.submit(_fetch_one, code) for code in codes]
        for fut in as_completed(futures):
            try:
                code, frame = fut.result()
            except Exception as exc:  # noqa: BLE001 — _retry already logged
                logger.warning("csi300 fetch worker raised: %s", exc)
                continue
            if frame is not None and not frame.empty:
                fetched[code] = frame

    # A name with no usable adjustment factors is dropped rather than benched on
    # raw prices, so the drop has to be visible or it becomes its own silent bias.
    dropped = sorted(set(codes) - set(fetched))
    if not fetched:
        raise RuntimeError(
            "csi300: no symbol survived corporate-action adjustment — "
            "pro.adj_factor returned nothing usable for any of the "
            f"{len(codes)} names, which usually means the Tushare token lacks "
            "adj_factor permission. Benching on unadjusted prices is not an "
            "alternative: an ex-date injects a fabricated cross-sectional "
            "return, measured at -47.2% on 300750.SZ 2023-04-26."
        )
    if dropped:
        logger.warning(
            "csi300: dropped %d/%d name(s) with no usable adjustment factors: %s",
            len(dropped),
            len(codes),
            ", ".join(dropped[:10]) + ("..." if len(dropped) > 10 else ""),
        )

    panel = _wide_from_fetched(fetched, include_amount=True)
    # CN equity vwap: Tushare ``amount`` is in 千元, ``volume`` in 手. True VWAP
    # = (amount * 1000 CNY) / (volume * 100 shares). Matches
    # ``src.factors.base.vwap(EQUITY_CN)``.
    if "amount" in panel and "volume" in panel:
        from src.factors.base import safe_div

        panel["vwap"] = safe_div(panel["amount"] * 1000.0, panel["volume"] * 100.0 + 1.0)

    # Restrict each date's cross-section to the names that were index members on
    # that date. Without this the panel carries today's roster back through the
    # whole window, so a name is only present because it survived to the end —
    # every IC is then measured on a set selected with hindsight.
    if membership is not None:
        mask = (
            membership.reindex(columns=panel["close"].columns)
            .reindex(index=panel["close"].index.union(membership.index))
            .ffill()
            .reindex(panel["close"].index)
            .bfill()
            .fillna(False)
            .astype(bool)
        )
        for key, frame in panel.items():
            if isinstance(frame, pd.DataFrame):
                panel[key] = frame.where(mask)

    panel["_meta"] = {
        "universe": "csi300",
        # True only on the degraded path: the hand-picked fallback is a
        # survivor-selected static roster with no point-in-time membership.
        "survivorship_bias": membership is None,
        "pit_membership": membership is not None,
        "degraded": constituent_source != "tushare index_weight",
        "constituent_source": constituent_source,
        "constituent_source_date": constituent_source_date,
        "constituent_count": len(codes),
        # Prices are corporate-action adjusted; raw pro.daily is not.
        "price_adjustment": "qfq",
        "dropped_unadjustable": len(dropped),
    }
    return panel
