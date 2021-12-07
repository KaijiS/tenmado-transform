import datetime
import logging

from utils import files
from utils import jinja2
from utils import bq
from utils.decorator import set_config

# loggerの設定
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@set_config
def fillna(config: dict):
    """
    週情報の翌日分が抜けている情報を数日予報の値でうめていく
    """

    # 該当日のレコードをチェック
    dt_now: datetime.datetime = datetime.datetime.now()
    dt_onedayago = dt_now - datetime.timedelta(days=1)
    yesterday_str: str = dt_onedayago.strftime("%Y-%m-%d")

    # t_week_weatherの補完
    tweekweather_query_base = files.read_file(
        "sqls/transformservice/t_week_weather_fillna.sql"
    )
    tweekweather_query = jinja2.embed_to_query(
        query_base=tweekweather_query_base,
        params=config
        | {
            "yesterday": yesterday_str,
        },
    )
    tweekweather_results = bq.exe_query(query=tweekweather_query)

    logger.info("[completed] week weather fillna")

    # t_week_tempsの補完
    tweektemps_query_base = files.read_file(
        "sqls/transformservice/t_week_temps_fillna.sql"
    )
    tweektemps_query = jinja2.embed_to_query(
        query_base=tweektemps_query_base,
        params=config
        | {
            "yesterday": yesterday_str,
        },
    )
    tweektemps_results = bq.exe_query(query=tweektemps_query)

    logger.info("[completed] week temps fillna")

    return


@set_config
def concat(config: dict):
    """
    週天気と週気温をjoinする
    """

    # 該当日のレコードをチェック
    dt_now: datetime.datetime = datetime.datetime.now()
    dt_onedayago = dt_now - datetime.timedelta(days=1)
    yesterday_str: str = dt_onedayago.strftime("%Y-%m-%d")

    query_base = files.read_file("sqls/transformservice/t_week_weather_temps.sql")
    query = jinja2.embed_to_query(
        query_base=query_base,
        params=config
        | {
            "yesterday": yesterday_str,
        },
    )
    results = bq.exe_query(query=query)

    logger.info("[completed] concat week weather and temps")

    return
