import datetime
import logging

import pandas as pd
from google.cloud import firestore

from utils import files
from utils import jinja2
from utils import bq
from utils.decorator import set_config

# loggerの設定
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@set_config
def insert_weekweather(config: dict) -> bool:
    """
    集計結果をfirestoreにinsertしていく
    """

    # 該当日のレコードをチェック
    dt_now: datetime.datetime = datetime.datetime.now(
        datetime.timezone(datetime.timedelta(hours=9))
    )
    dt_onedayago = dt_now - datetime.timedelta(days=1)
    yesterday_str: str = dt_onedayago.strftime("%Y-%m-%d")

    query_base = files.read_file("sqls/firestoreservice/selectprocessedweek.sql")

    query = jinja2.embed_to_query(
        query_base=query_base,
        params=config
        | {
            "yesterday": yesterday_str,
        },
    )

    result_df = bq.to_dataframe(query=query)

    # firestoreのコレクション準備
    """
    collection: weekweather
    - doc: {large_area_code}_{report_datetime}
        - get_datetime: xxxx/xx/xx OO:OO:OO
        - report_datetime: xxxx/xx/xx OO:OO:OO
        - "meteorological_observatory_name": "〇〇気象台"
        - large_area_code: xxx
        - large_area_name: xxx
        - city_code: xxx
        - city_name: xxx
        - collection: forecasts
            - doc: {large_area_code}_{report_datetime}_{forecast_target_date}
                - "forecast_target_date": "2021-10-11",
                - "weather_code": "",
                - "pop": "",
                - "reliability": "",
                - "lowest_temperature": 12.1,
                - "lowest_temperature_lower": 10.5,
                - "lowest_temperature_upper": 13.1,
                - "highest_temperature": 12.1,
                - "highest_temperature_lower": 10.5,
                - "highest_temperature_upper": 13.1
    """
    db = firestore.Client()
    weekweather_collection = db.collection("weekweather")

    large_area_codes = result_df["large_area_code"].unique()
    for large_area_code in large_area_codes:

        large_area_result_df = result_df[
            result_df["large_area_code"] == large_area_code
        ].copy()

        weekweather_doc_content = {}
        weekweather_doc_content["get_datetime"] = pd.to_datetime(
            large_area_result_df["get_datetime"].values[0]
        )
        weekweather_doc_content["report_datetime"] = pd.to_datetime(
            large_area_result_df["report_datetime"].values[0]
        )
        weekweather_doc_content[
            "meteorological_observatory_name"
        ] = large_area_result_df["meteorological_observatory_name"].values[0]
        weekweather_doc_content["large_area_code"] = large_area_result_df[
            "large_area_code"
        ].values[0]
        weekweather_doc_content["large_area_name"] = large_area_result_df[
            "large_area_name"
        ].values[0]
        weekweather_doc_content["city_code"] = large_area_result_df["city_code"].values[
            0
        ]
        weekweather_doc_content["city_name"] = large_area_result_df["city_name"].values[
            0
        ]

        # コレクションにドキュメントを追加
        weekweather_doc_name = "{}_{}".format(
            weekweather_doc_content["report_datetime"].strftime("%Y%m%d"),
            weekweather_doc_content["large_area_code"],
        )
        weekweather_collection.document(weekweather_doc_name).set(
            weekweather_doc_content
        )

        # そのドキュメントの中にサブコレクションを作成し、各予報対象日のデータを格納
        forecasts_subcollection = weekweather_collection.document(
            weekweather_doc_name
        ).collection("forecasts")
        for index, row in large_area_result_df.iterrows():
            forecast_doc_content = {}
            forecast_doc_content["forecast_target_date"] = pd.to_datetime(
                row["forecast_target_date"]
            )
            forecast_doc_content["weather_code"] = row["weather_code"]
            forecast_doc_content["pop"] = row["pop"]
            forecast_doc_content["reliability"] = row["reliability"]
            forecast_doc_content["lowest_temperature"] = row["lowest_temperature"]
            forecast_doc_content["lowest_temperature_lower"] = row[
                "lowest_temperature_lower"
            ]
            forecast_doc_content["lowest_temperature_upper"] = row[
                "lowest_temperature_upper"
            ]
            forecast_doc_content["highest_temperature"] = row["highest_temperature"]
            forecast_doc_content["highest_temperature_lower"] = row[
                "highest_temperature_lower"
            ]
            forecast_doc_content["highest_temperature_upper"] = row[
                "highest_temperature_upper"
            ]

            forecast_doc_name = "{}_{}".format(
                weekweather_doc_name,
                forecast_doc_content["forecast_target_date"].strftime("%Y%m%d"),
            )
            forecasts_subcollection.document(forecast_doc_name).set(
                forecast_doc_content
            )

    logger.info("[completed] insert weekweather to firestore")

    return True


@set_config
def insert_molargearea(config: dict) -> bool:

    query_base = files.read_file("sqls/firestoreservice/selectmolargearea.sql")

    query = jinja2.embed_to_query(query_base=query_base, params=config)

    result_df = bq.to_dataframe(query=query)

    db = firestore.Client()
    largearea_collection = db.collection("largearea")

    for index, row in result_df.iterrows():

        largearea_collection.document(row["large_area_code"]).set(
            {
                "meteorological_observatory_name": row[
                    "meteorological_observatory_name"
                ],
                "large_area_code": row["large_area_code"],
                "large_area_name": row["large_area_name"],
            }
        )

    logger.info("[completed] insert molargearea to firestore")

    return True
