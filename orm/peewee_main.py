import traceback
from peewee import PostgresqlDatabase

from common.system_util import SystemUtil
from common.base64_util import Base64Util
from orm.peewee_models.event_forecast_result import *


py_config = SystemUtil.get_py_config()
pg_decode_config = Base64Util.get_config_decode_value(py_config['postgres'])


db = PostgresqlDatabase(
    pg_decode_config['database'],
    user=pg_decode_config['id'],
    password=pg_decode_config['password'],
    host=pg_decode_config['host'],
    port=pg_decode_config['port']
)


def connect_db():
    # 데이터베이스 연결
    db.connect()


def close_db():
    # 데이터베이스 연결 해제
    db.close()


def insert_eventfcst(data: list, logger: object) -> None:
    """
    :param data: 이슈예측 서빙 결과 리스트
    :param logger: python logger
    :return:

    insert_eventfcst 동작 Flow
    1) performance 테이블에서 result_eventfcst_performance_id 값이 있는지 조회
    1-1) result_eventfcst_performance_id 값이 없으면 performance 테이블에 insert
    2) 1에서 조회한 result_eventfcst_performance_id값으로 probability, summary 테이블에 입력할 리스트 append
    3) probability, summary 테이블에 bulk insert
    """
    probability = []
    summary = []

    try:
        for i in data:
            keys = i['body']['event_fcst']['keys']
            values = i['body']['event_fcst']['values']

            for value in values:
                prediction_data = dict(zip(keys, value))
                event_result = [item for item in prediction_data["event_prob"] if item.get('event_result')]

                # 서빙 결과에 이벤트예측이 있는 경우에만 저장함
                if event_result:
                    # performance
                    try:
                        existing_record = AiResultEventfcstPerformance.get(
                            (AiResultEventfcstPerformance.serving_time == prediction_data["time"]) &
                            (AiResultEventfcstPerformance.predict_time == prediction_data["predict_time"]) &
                            (AiResultEventfcstPerformance.group_type == prediction_data["group_type"]) &
                            (AiResultEventfcstPerformance.group_id == prediction_data["group_id"]) &
                            (AiResultEventfcstPerformance.event_code == prediction_data["event_code"])
                        )
                        p_id = existing_record.result_eventfcst_performance_id
                    except:
                        record = AiResultEventfcstPerformance.create(
                            serving_time=prediction_data["time"],
                            predict_time=prediction_data["predict_time"],
                            group_type=prediction_data["group_type"],
                            group_id=prediction_data["group_id"],
                            event_code=prediction_data["event_code"],
                            event_doc=prediction_data["event_doc"]
                        )
                        p_id = record.result_eventfcst_performance_id

                    # probability
                    proba_obj = [AiResultEventfcstProbability(
                        serving_time=prediction_data["time"],
                        result_eventfcst_performance_id=p_id,
                        inst_type=event["inst_type"],
                        target_id=event["target_id"],
                        event_prob=event["event_prob"]
                    ) for event in event_result]
                    probability.extend(proba_obj)

                    # summary
                    summary_obj = [AiResultEventfcstSummary(
                        serving_time=prediction_data["time"],
                        result_eventfcst_performance_id=p_id,
                        inst_type=metric["inst_type"],
                        target_id=metric["target_id"],
                        metric_name=metric["metric_name"],
                        contribution=metric["contribution"]
                    ) for metric in prediction_data["explain"][0]]
                    summary.extend(summary_obj)
    except Exception:
        tb = traceback.format_exc()
        logger.info(f"insert_eventfcst data parsing error: {tb}")

    try:
        with db.atomic():
            AiResultEventfcstProbability.bulk_create(probability, batch_size=100)
            AiResultEventfcstSummary.bulk_create(summary, batch_size=100)
    except Exception:
        tb = traceback.format_exc()
        logger.info(f"insert_eventfcst bulk_create error: {tb}")
