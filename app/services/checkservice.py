import datetime
import logging

from utils.config import get_config
from utils import files
from utils import jinja2
from utils import bq
from utils.decorator import set_config

# loggerの設定
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@set_config
def check_load(config: dict):
    """
    ロード処理が成功したか否かをチェック
    report_datetimeのレコードの有無で確認
    """

    # 該当日のレコードをチェック
    dt_now: datetime.datetime = datetime.datetime.now()
    dt_onedayago = dt_now - datetime.timedelta(days=1)
    yesterday_str: str = dt_onedayago.strftime("%Y-%m-%d")

    query_base = files.read_file("sqls/checkservice/checkload.sql")

    check_flag = True

    for table_name in config["import_table_names"]:

        query = jinja2.embed_to_query(
            query_base=query_base,
            params=config
            | {
                "table_name": table_name,
                "yesterday": yesterday_str,
            },
        )

        results = bq.exe_query(query)

        record_count = [row.cnt for row in results][0]

        if record_count == 0:
            logger.warning("[completed] check load: no record")
            check_flag = False
        else:
            logger.info("[completed] check load: has records")

    return check_flag
