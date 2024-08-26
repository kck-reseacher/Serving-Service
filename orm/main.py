from tortoise import Tortoise, run_async
import urllib.parse

from orm.models.anomaly_detect_result_model import ResultGdnSummary, ResultGdnPerformance
from orm.models.forecast_result_model import ResultTsmixerPredict, ResultRmcPredict
from orm.models.event_forecast_result_model import ResultEventfcstPerformance, ResultEventfcstSummary, ResultEventfcstProbability
from common.system_util import SystemUtil
from common.base64_util import Base64Util

py_config = SystemUtil.get_py_config()
pg_decode_config = Base64Util.get_config_decode_value(py_config['postgres'])
encoded_password = urllib.parse.quote_plus(pg_decode_config['password'])
DATABASE_URL = f"postgres://{pg_decode_config['id']}:{encoded_password}@{pg_decode_config['host']}:{pg_decode_config['port']}/{pg_decode_config['database']}"


def init_mldb():
    """
    tortoise init 함수 비동기 실행
    """
    run_async(initial())


async def initial():
    """
    tortoise db connection 부터 테이블 모델, 기타 설정 사항을 초기화
    """
    await Tortoise.init(
        config={
            'connections': {
                'default': DATABASE_URL
            },
            'apps': {
                'models': {
                    'models': [
                        'orm.models.anomaly_detect_result_model',
                        'orm.models.forecast_result_model',
                        'orm.models.event_forecast_result_model'
                    ],
                    'default_connection': 'default',
                }
            },
            'use_tz': False
        }
    )


# < ================================== query parsing ================================== >
async def insert_gdn(data: list) -> None:
    """
    이상탐지 GDN 알고리즘 추론 결과를 관련 테이블에 insert 하기 전 query 문 전처리하고 insert 함수를 호출

    :param data = ai 추론 결과인 response 를 모아둔 list
    :return 현재까지 없음
    """
    summary = []
    performances = []
    try:
        for i in data:
            summary.append(ResultGdnSummary(
                time=i['body']['detect_gdn']["time"],
                target_id=i['body']['detect_gdn']["target_id"],
                inst_type=i['body']['detect_gdn']['inst_type'],
                is_anomaly=i['body']['detect_gdn']['is_anomaly'],
                normality_score=i['body']['detect_gdn']['normality_score'],
                gdn_grade=i['body']['detect_gdn']['target_grade']
            ))

            performance = [ResultGdnPerformance(
                time=i['body']['detect_gdn']["time"],
                target_id=i['body']['detect_gdn']["target_id"],
                inst_type=i['body']['detect_gdn']["inst_type"],
                real_value=m['real_value'],
                metric=m['metric_name'],
                attention_map=m["attention_map"],
                model_value=m["model_value"],
                anomaly_contribution=m["anomaly_contribution"]
            ) for m in i['body']['detect_gdn']['metric_list']]
            performances.extend(performance)
    except Exception as e:
        raise Exception(f"data parsing error : {e}")

    try:
        await bulk_insert_gdn_summary(summary)  # gdn_summary 테이블 bulk insert
    except Exception as e:
        raise Exception(f"fail to bulk_insert_gdn_summary : {e}")
    try:
        await bulk_insert_gdn_performance(performances)  # gdn_performance 테이블 bulk insert
    except Exception as e:
        raise Exception(f"fail to bulk_insert_gdn_performance : {e}")


async def insert_eventfcst(data: list) -> None:
    """
    이벤트 예측 GAT 추론 결과를 관련 테이블에 insert 하기 전 query 문 전처리하고 insert 함수를 호출

    :param data = ai 추론 결과인 response 를 모아둔 list
    :return None
    """
    probability = []
    summary = []
    # data parsing
    try:
        for i in data:
            keys = i['body']['event_fcst']['keys']
            values = i['body']['event_fcst']['values']

            for value in values:
                prediction_data = dict(zip(keys, value))
                event_result = [item for item in prediction_data["event_prob"] if item.get('event_result')]

                # 서빙 결과에 이벤트예측이 있는 경우에만 저장함
                if event_result:
                    try:
                        existing_record = await ResultEventfcstPerformance.get(
                            serving_time=prediction_data["time"],
                            predict_time=prediction_data["predict_time"],
                            group_type=prediction_data["group_type"],
                            group_id=prediction_data["group_id"],
                            event_code=prediction_data["event_code"]
                        )
                        p_id = existing_record.result_eventfcst_performance_id
                    except:
                        record = await ResultEventfcstPerformance.create(
                            serving_time=prediction_data["time"],
                            predict_time=prediction_data["predict_time"],
                            group_type=prediction_data["group_type"],
                            group_id=prediction_data["group_id"],
                            event_code=prediction_data["event_code"],
                            event_doc=prediction_data["event_doc"]
                        )

                        p_id = record.result_eventfcst_performance_id

                    # proba
                    proba_obj = [ResultEventfcstProbability(
                        serving_time=prediction_data["time"],
                        result_eventfcst_performance_id=p_id,
                        inst_type=event["inst_type"],
                        target_id=event["target_id"],
                        event_prob=event["event_prob"]
                    ) for event in event_result]
                    probability.extend(proba_obj)

                    # summary
                    summary_obj = [ResultEventfcstSummary(
                        serving_time=prediction_data["time"],
                        result_eventfcst_performance_id=p_id,
                        inst_type=metric["inst_type"],
                        target_id=metric["target_id"],
                        metric_name=metric["metric_name"],
                        contribution=metric["contribution"]
                    ) for metric in prediction_data["explain"][0]]
                    summary.extend(summary_obj)


    except Exception as e:
        raise Exception(f"data parsing error : {e}")

    try:
        await bulk_insert_event_summary(summary)
    except Exception as e:
        raise Exception(e)

    try:
        await bulk_insert_event_probability(probability)
    except Exception as e:
        raise Exception(e)


async def insert_tsmixer(data: list) -> None:
    """
    부하예측 tsmixer 추론 결과를 관련 테이블에 insert 하기 전 query 문 전처리하고 insert 함수를 호출

    :param data = ai 추론 결과인 response 를 모아둔 list
    :return None
    """
    predictions = []
    try:
        for i in data:
            keys = i['body']['pred_tsmixer']['keys']
            values = i['body']['pred_tsmixer']['values']

            for value in values:
                prediction_data = dict(zip(keys, value))
                prediction = ResultTsmixerPredict(
                    time=prediction_data['predict_time'],
                    diff_minute=prediction_data['diff_minute'],
                    target_id=prediction_data['target_id'],
                    inst_type=prediction_data['inst_type'],
                    metric=prediction_data['metric'],
                    value=prediction_data['predict_value'],
                    serving_time=prediction_data['serving_time']
                )
                predictions.append(prediction)
    except Exception as e:
        raise Exception(f"data parsing error : {e}")

    try:
        await bulk_insert_tsmixer(predictions)
    except Exception as e:
        raise Exception(f"fail to bulk_insert_tsmixer : {e}")


async def insert_rmc(data: dict) -> None:
    """
    부하예측 rmc 추론 결과를 관련 테이블에 insert 하기 전 query 문 전처리하고 insert 함수를 호출

    :param data = ai 추론 결과인 response 를 모아둔 list
    :return None
    """
    objs = []
    try:
        keys = data['body']['predict_seq2seq']['keys']
        values = data['body']['predict_seq2seq']['values']
        for i in values:
            obj = [ResultRmcPredict(
                time=dd['time'],
                diff_minute=idx+1,
                target_id=i[keys.index("target_id")],
                inst_type=i[keys.index("type")],
                metric=i[keys.index("name")],
                value=dd['value'],
                serving_time=i[keys.index("time")]
            ) for idx, dd in enumerate(i[keys.index('predict_value')]['data'])]

            objs.extend(obj)
    except Exception as e:
        raise Exception(f"data parsing error : {e}")

    try:
        await bulk_insert_rmc(objs)
    except Exception as e:
        raise Exception(f"fail to bulk_insert_rmc : {e}")


# <------------------------------------- 이상탐지 ------------------------------------->
async def bulk_insert_gdn_summary(records: list) -> None:
    """
    ai_result_gdn_summary 테이블 insert
    """
    await ResultGdnSummary.bulk_create(records)
    print("gdn_summary insert success.")


async def bulk_insert_gdn_performance(records: list) -> None:
    """
    ai_result_gdn_performance 테이블 insert
    """
    await ResultGdnPerformance.bulk_create(records)
    print("gdn_performance insert success.")


# <------------------------------------- 이벤트 예측 ------------------------------------->
async def bulk_insert_event_summary(records: list) -> None:
    """
    ai_result_eventfcst_summary 테이블 insert
    """
    await ResultEventfcstSummary.bulk_create(records)
    print("event_summary insert success.")


async def bulk_insert_event_performance(records: list) -> None:
    """
    ai_result_eventfcst_summary 테이블 insert
    """
    await ResultEventfcstPerformance.bulk_create(records)
    print("event_performance insert success.")


async def bulk_insert_event_probability(records: list) -> None:
    """
    ai_result_eventfcst_summary 테이블 insert
    """
    await ResultEventfcstProbability.bulk_create(records)
    print("event_performance insert success.")


# <------------------------------------- 부하예측 ------------------------------------->
async def bulk_insert_tsmixer(records: list) -> None:
    """
    ai_result_tsmixer_predict 테이블 insert
    """
    await ResultTsmixerPredict.bulk_create(records)
    print("tsmixer_predict insert success.")


async def bulk_insert_rmc(records: list) -> None:
    """
    ai_result_seq2seq_predict 테이블 insert
    """
    await ResultRmcPredict.bulk_create(records)
    print("rmc_predict insert success.")
