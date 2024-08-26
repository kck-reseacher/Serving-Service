import traceback
from collections import deque
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from pathlib import Path

from resources.config.kafka_config_dev import KafkaConfig
from resources.logger_manager import Logger
from common.redis_client import *
from common.system_util import SystemUtil

from util_pg import *
from orm.main import init_mldb, insert_gdn, insert_tsmixer, insert_rmc
from orm.peewee_main import close_db, insert_eventfcst

import asyncio
import json
import multiprocessing
import setproctitle
import re
import time


async def consume(conf: dict, topic: str, sys_id: int, inst_type: str, logger: object) -> None:
    """
    카프카 토픽을 구독하고 메세지를 지속적으로 컨슈밍함

    :param conf = 카프카 컨슈머 설정 정보
    :param topic = 토픽 정보
    :param sys_id = 시스템 아이디
    :param inst_type = 타입 정보
    :param logger = 파이썬 로깅 객체
    :return = None
    """
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    window_size = 60
    offset_step = 2  # offset_step에 따라 하드코딩
    offset_threshold = 150
    req = deque(maxlen=window_size)

    product_type_mapping = {
        KafkaConfig.db_oracle_topic: "ORACLE",
        KafkaConfig.db_postgres_topic: "POSTGRES",
        KafkaConfig.db_tibero_topic: "TIBERO"
    }

    try:
        """
        과거 데이터 처리 방법
        Last offset: 특정 파티션에 존재하는 가장 최신 메시지의 오프셋
        Consumer offset: 컨슈머 그룹이 마지막으로 커밋한 오프셋
        Lag = Last offset - consumer offset
        lag가 120(약1 ~ 2시간) 미만일 경우 consumer offset부터 처리
        lag가 120보다 클 경우 (last offset - offset step)부터 처리
        """
        start_time = time.time()
        timeout = 120
        while True:
            if time.time() - start_time > timeout:
                logger.error(f"[{topic}] Timeout processing historical offset")
                break

            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if handle_error(msg, consumer, topic, conf, window_size, logger):
                    continue

            if msg is not None:
                partition = 0
                tp = TopicPartition(topic, partition)
                low, last_offset = consumer.get_watermark_offsets(tp)
                lag = last_offset - msg.offset()
                if lag > 120:
                    logger.info(
                        f"topic: {topic}, lag exceed {lag}, assign consumer at offset: {last_offset - offset_step}")
                    consumer.assign([TopicPartition(topic, partition, last_offset - offset_step)])
                else:
                    logger.info(f"topic: {topic}, lag less than {lag}")
                break

        while True:
            msg = consumer.poll(timeout=5.0)

            if msg is None:
                logger.debug(f"[{topic}] consuming... wait")
                continue

            if msg.error():
                if handle_error(msg, consumer, topic, conf, window_size, logger):
                    continue

            # message 처리
            logger.info(f"{msg.topic()} [New message] offset : {msg.offset()}")

            """
            최초 기동시 window queue 를 채우는 작업 (과거 offset으로 이동)
            현재 offset 보다 window_size 이전 (과거) offset 의 message 부터 읽어옴
            """
            if len(req) < window_size:
                if msg.offset() > offset_threshold:
                    start_offset = find_60th_offset_from_current(consumer, msg.topic(), msg.partition(), msg.offset(),
                                                                 offset_threshold, logger)
                else:
                    start_offset = msg.offset()
                process_messages(consumer, msg.topic(), msg.partition(), start_offset, window_size, req, logger)
                product_type = product_type_mapping[msg.topic()] if inst_type == "db" else None
                await do_serving(req, sys_id, inst_type, logger, product_type)

            else:
                # message window slide
                body = json.loads(msg.value().decode('utf-8'))
                req.append(body)
                consumer.commit()

                product_type = product_type_mapping[msg.topic()] if inst_type == "db" else None
                await do_serving(req, sys_id, inst_type, logger, product_type)

    except KeyboardInterrupt:
        logger.info(f"{topic} system stop from ctrl+c ")

    except Exception as e:
        tb = traceback.format_exc()
        logger.info(f"{topic} [Unknown consumer process error] : {e}")
        logger.error(tb)

    finally:
        consumer.close()


def event_consume(conf: dict, topics: list, sys_id: int, inst_type: str, logger: object) -> None:
    """
    이벤트 예측 전용 context 가진 컨슈머
    카프카 토픽을 구독하고 메세지를 지속적으로 컨슈밍함

    :param conf = 카프카 컨슈머 설정 정보
    :param topics = 토픽 정보 리스트
    :param sys_id = 시스템 아이디
    :param inst_type = 타입 정보
    :param logger = 파이썬 로깅 객체
    :return = None
    """
    consumer = Consumer(conf)
    consumer.subscribe(topics)
    window_size = 60
    offset_step = 2  # offset_step에 따라 하드코딩
    offset_threshold = 150

    was_req = deque(maxlen=window_size)
    db_oracle_req = deque(maxlen=window_size)
    db_postgres_req = deque(maxlen=window_size)
    db_tibero_req = deque(maxlen=window_size)
    os_req = deque(maxlen=window_size)
    web_req = deque(maxlen=window_size)
    tp_req = deque(maxlen=window_size)

    topic_mapping = {
        KafkaConfig.was_topic: ("was", was_req),
        KafkaConfig.db_oracle_topic: ("db", db_oracle_req),
        KafkaConfig.db_postgres_topic: ("db", db_postgres_req),
        KafkaConfig.db_tibero_topic: ("db", db_tibero_req),
        KafkaConfig.os_topic: ("os", os_req),
        KafkaConfig.web_topic: ("web", web_req),
        KafkaConfig.tp_topic: ("tp", tp_req)
    }

    def handle_topic(consumer: object, msg: object, topic: str, req: deque, sys_id: int, logger: object) -> None:
        """
        이슈 예측 전용 토픽 별로 메시지 처리 및 서빙
        :param consumer = 카프카 컨슈머
        :parma msg = 카프카 토픽 메시지
        :param topic = 토픽 이름 ex) dm_was
        :param req = 서빙 데이터 queue
        :param sys_id = 시스템 아이디
        :param logger = 파이썬 로깅 객체
        :return = None
        """
        if len(req) < window_size:
            if end_offsets[topic] > offset_threshold:
                start_offset = find_60th_offset_from_current(consumer, topic, msg.partition(), end_offsets[topic],
                                                             offset_threshold, logger)
            else:
                start_offset = end_offsets[topic]
            process_messages(consumer, topic, msg.partition(), start_offset, window_size, req, logger, 'event-fcst')
            consumer.subscribe(topics)
        else:
            message_window_slide(msg, req, consumer)
            consumer.subscribe(topics)
            do_event_serving(req, sys_id, topic_mapping[topic][0], logger)


    try:
        # 가장 최신 offset 으로 이동
        partitions = []
        for topic in topics:
            partitions.extend([TopicPartition(topic, p) for p in consumer.list_topics(topic).topics[topic].partitions])
        consumer.assign(partitions)
        end_offsets = get_end_offsets(consumer, partitions, offset_step)

        while True:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                logger.debug(f"[{inst_type}] consuming... wait")
                continue
            if msg.error():
                if handle_error(msg, consumer, msg.topic(), conf, window_size, logger):
                    continue

            logger.info(f"event_consumer, {msg.topic()} [New message] offset : {msg.offset()}")

            # 어떤 topic의 메세지인지 구분
            msg_topic = msg.topic()
            if msg_topic in topic_mapping:
                instance_type, req = topic_mapping[msg_topic]
                handle_topic(consumer, msg, msg_topic, req, sys_id, logger)

            # 가장 최근 메세지들로 window 구성했으니, 모두 동일 t시점 time sync 라고 가정

    except KeyboardInterrupt:
        logger.info(f"{topic} system stop from ctrl+c ")

    except Exception as e:
        tb = traceback.format_exc()
        logger.info(f"{topic} [Unknown consumer process error] : {e}")
        logger.error(tb)

    finally:
        close_db()
        consumer.close()

def handle_error(msg: object, consumer: object, topic: str, conf: dict, window_size: int, logger: object):
    """
    :param msg: kafka message
    :param consumer: kafka consumer
    :param topic: kafka topic 명, ex) 'dm_was'
    :param conf: kafka config ex) bootstrap.servers, group_id
    :param window_size: 서빙 데이터 window size
    :param logger: python logger
    메세지 에러 예외처리 케이스
    1) partition eof : 메세지를 끝까지 읽은 경우 -> 계속해서 컨슈밍 (continue)
    2) max polling : 메세지 폴링 지연의 경우 -> 접속을 종료하고 재구독
    3) out of range : 가져온 메세지가 카프카 정책에 의해 삭제된 경우 -> 오프셋을 조정 (continue)
    """
    if msg.error().code() == KafkaError._PARTITION_EOF:
        logger.warning(f"{topic} [partition eof] reached end at offset {msg.offset()}\n.")
        return True
    elif msg.error().code() == KafkaError._MAX_POLL_EXCEEDED:
        logger.warning(f"{topic} [max polling] consumer closing and re subscribe.")
        consumer.close()
        consumer = Consumer(conf)
        consumer.subscribe([topic])
        return True
    elif msg.error().code() == KafkaError.OFFSET_OUT_OF_RANGE:
        tp = TopicPartition(msg.topic(), msg.partition(), offset=-2)
        offsets = consumer.get_watermark_offsets(tp, timeout=10.0)
        logger.warning(f"{topic} [out of range] removed offset, moving offset to {offsets[0]}.")
        start_offset = offsets[0] + window_size
        consumer.assign([TopicPartition(topic, msg.partition(), start_offset)])
        return True
    else:
        logger.error(f"{topic} [unknown polling error] error : {msg.error()}")
        raise KafkaException(msg.error())

def find_60th_offset_from_current(consumer: object, topic: str, partition: int, current_offset: int, offset_threshold: int, logger: object) -> int:
    """

    :param consumer: kafka consumer
    :param topic: kafka topic 명, ex) 'dm_was'
    :param partition: kafka topic partition
    :param current_offset: 최신 offset
    :param offset_threshold: offset step(2)에 대응한 임계값 ex) 150
    :param logger: python logger
    :return: 최신 offset으로부터 과거 60번째 offset 번호

    동작 Flow
    1) 현재 offset 번호에서 threshold(150)만큼 뺀 offset 번호로 consumer assign
    2) 과거 (offset - threshold) offset부터 현재 offset까지 polling
    3) 현재 offset에서 과거 60번째 offset 번호를 return
    """
    start_time = time.time()
    timeout = 30
    offsets = []
    consumer.assign([TopicPartition(topic, partition, current_offset - offset_threshold)])

    while True:
        if time.time() - start_time > timeout:
            logger.error(f"[{topic}] Timeout find_60th_offset_from_current while waiting for messages")
            break

        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            logger.warning(f"Error reading message at offset {msg.offset()}: {msg.error()}")
            continue
        if msg.offset() not in offsets:
            offsets.append(msg.offset())
        if current_offset in offsets:
            break

    if len(offsets) < 60:
        logger.error(f"[{topic}] Not enough messages to determine the 60 length offset")
        return current_offset

    return offsets[-60]

def process_messages(consumer: object, topic: str, partition: int, start_offset: int, window_size: int, req: deque, logger: object, module=None) -> None:
    """
    :param consumer: kafka consumer object
    :param topic: kafka topic 명
    :param partition: kafka topic partition number
    :param start_offset: window_size 만큼 컨슈밍하기 위한 offset 시작 번호
    :param window_size: 서빙 데이터 window size
    :param req: 서빙 데이터 queue
    :param logger: python logger
    :param module: XAIOps 기능, ex) event-fcst
    :return:

    process_messages 동작 Flow
    1) start_offset 기준으로 consumer 재할당
    2) window_size 만큼 컨슈밍
    3) 컨슈밍 완료 후 commit
    4) 이슈 예측을 제외하고 커밋 후 다시 토픽 구독
    """
    consumer.assign([TopicPartition(topic, partition, start_offset)])

    for _ in range(window_size):
        historic_msg = consumer.poll(timeout=1.0)
        if historic_msg is None:
            continue
        if historic_msg.error():
            logger.warning(f"Error reading message at offset {historic_msg.offset()}: {historic_msg.error()}")
            continue
        body = json.loads(historic_msg.value().decode('utf-8'))
        req.append(body)

    consumer.commit()
    if module is None:
        consumer.subscribe([topic])  # 다시 원래의 파티션과 최신 offset을 구독


def get_end_offsets(consumer: object, partitions: object, offset_step: int) -> int:
    """
    카프카 토픽의 최신 offset number를 리턴

    :param consumer: kafka consumer object
    :param partitions: kafka partition object
    :param offset_step: offset 번호 증가 단위 ex) 1 or 2
    :return: 가장 최신의 offset 번호
    """
    end_offsets = {}
    for tp in partitions:
        low, latest = consumer.get_watermark_offsets(tp)
        end_offsets[tp.topic] = latest - offset_step
    return end_offsets


def message_window_slide(msg: object, msg_que: deque, consumer: object) -> None:
    """
    서빙 메시지 queue에 최신 메시지를 추가

    :param msg: kafka topic message
    :param msg_que: queue 형태의 서빙 데이터
    :param consumer: kafka consumer object
    :return:
    """
    body = json.loads(msg.value().decode('utf-8'))
    msg_que.append(body)
    consumer.commit()


async def do_serving(req: deque, sys_id: int, inst_type: str, logger: object, product_type) -> None:
    """
    :param req : kafka message queue (window)
    :param sys_id : 시스템 아이디 개발 102 운영 2
    :param inst_type : 인스턴스 타입
    :param logger : python logging object
    :param product_type : DB 인스턴스 product type ex) 'ORACLE'

    do_serving 동작 Flow.
    1) get_serving_targets() : Redis 로 부터 서빙 대상 조회
    2) message_filtering() : kafka 에서 가져온 전체 대상 중 서빙 대상을 필터링
    3) serving_request() : 비동기 request 전송 (멀티스레딩)
    4) send_request() : 동기 request 전송
    5) insert() : 추론 결과 response 를 각 매핑 테이블에 저장 (bulk_insert)
    """
    try:
        # --------------------- 이상탐지 ----------------------
        inst_target_list = await get_serving_targets('exem_aiops_anls_inst', inst_type)
        if inst_target_list:
            anomaly_req_list = await message_filtering(req, 'exem_aiops_anls_inst', inst_target_list, sys_id, inst_type)
            if anomaly_req_list:
                anomaly_res_list = await serving_request(anomaly_req_list, logger)
                if anomaly_res_list:
                    await insert_gdn(anomaly_res_list)

        # ------------------- 부하예측(그룹) -------------------
        if inst_type == "db":  # 하위 그룹이 있는 경우
            system_fcst_target_list = []
            targets = await get_serving_targets_load_fcst(inst_type, product_type)
            if targets:
                system_fcst_target_list.extend(targets)
        else:  # other instance type
            system_fcst_target_list = await get_serving_targets_load_fcst(inst_type)
        if system_fcst_target_list:
            loadfcst_req_list = await message_filtering_for_system(req, 'exem_aiops_load_fcst', system_fcst_target_list, sys_id, inst_type, product_type)
            if loadfcst_req_list:
                loadfcst_res_list = send_request(loadfcst_req_list, logger)
                if loadfcst_res_list:
                    await insert_rmc(loadfcst_res_list)

        # ------------------- 부하예측(타겟) -------------------
        fcst_target_list = await get_serving_targets('exem_aiops_fcst_tsmixer', inst_type)
        if fcst_target_list:
            fcst_req_list = await message_filtering(req, 'exem_aiops_fcst_tsmixer', fcst_target_list, sys_id, inst_type)
            if fcst_req_list:
                fcst_res_list = await serving_request(fcst_req_list, logger)
                if fcst_res_list:
                    await insert_tsmixer(fcst_res_list)

    except Exception:
        tb = traceback.format_exc()
        logger.error(f"[Unknown serving error] : {tb}")


def do_event_serving(req: deque, sys_id: int, inst_type: str, logger: object) -> None:
    """
    이슈 예측 전용 서빙 함수
    :param req: 서빙 메시지 queue
    :param sys_id: XAIOps system id
    :param inst_type: XAIOps Instance type
    :param logger: python logger
    :return:

    do_event_serving 동작 Flow
    1) redis server에서 이슈예측 redis key값 조회
    2) redis server에서 이슈예측 타겟에 맵핑된 타겟 리스트를 조회
    3) redis key 값에서 이슈예측 그룹의 타입과 그룹 아이디를 파싱
    4) 서빙 요청 및 결과 저장
    """
    try:
        # -------------------- 이벤트예측 --------------------
        serving_req_list = []
        eventfcst_keylist = get_rediskey_eventfcst_sync(inst_type)
        for key in eventfcst_keylist:
            target_list = get_serving_targets_eventfcst_sync(key, inst_type)
            if target_list is not None:
                group_type, group_id = get_grouptarget_eventfcst_sync(key)
                eventfcst_req_list = message_filtering_sync(req, 'exem_aiops_event_fcst', target_list, sys_id, inst_type, group_type, group_id)
                if eventfcst_req_list:
                    serving_req_list.extend(eventfcst_req_list)

        if serving_req_list:
            eventfcst_res_list = serving_request_sync(serving_req_list, logger)
            if eventfcst_res_list:
                insert_eventfcst(eventfcst_res_list, logger)
                logger.info(f"event-fcst {inst_type} insert")

    except Exception:
        tb = traceback.format_exc()
        logger.error(f"[Unknown serving error] : {tb}")


def main(config: dict, topic: str, sys_id: int, inst_type: str) -> None:
    """
    python logger 를 생성하고 비동기 코루틴 consume 메서드를 실행함
    이슈 예측의 경우 동기 방식의 event_consume 메서드를 실행

    :param config = 카프카 컨슈머 설정 정보
    :param topic = 카프카 토픽명
    :param sys_id = 시스템 아이디
    :param inst_type = 인스턴스 타입 구분
    """
    if inst_type == 'db':
        pattern = re.compile(r'^dm_')
        parsed_value = pattern.sub('', topic)
        setproctitle.setproctitle(f"consumer_{parsed_value}")
    else:
        setproctitle.setproctitle(f"consumer_{inst_type}")
    log_path = get_server_run_configuration()

    error_log_dict = dict()
    error_log_dict["log_dir"] = str(Path(log_path) / "srs" / "Error")
    error_log_dict["file_name"] = "consumer"
    logger = Logger().get_default_logger(logdir=log_path + "/srs/consumer", service_name="consumer",
                                         error_log_dict=error_log_dict)

    if inst_type == "event-fcst":
        event_consume(config, topic, sys_id, inst_type, logger)
    else:
        asyncio.run(consume(config, topic, sys_id, inst_type, logger))


if __name__ == "__main__":
    """
    (develop) Kafka 설정 정보를 가져와 각 dm topic 을 멀티 프로세스로 독립적으로 컨슈밍함
    """
    conf = KafkaConfig.load_config()
    event_conf = KafkaConfig.load_event_config()
    topic_conf = KafkaConfig.load_topic_config()
    topics = []
    for instance_type, data in topic_conf.items():
        topic_list = data.get('topics', [])
        for topic in topic_list:
            topics.append((instance_type, topic))

    py_config = SystemUtil.get_py_config()
    sys_id = py_config["sys_id"]

    consumers = [multiprocessing.Process(target=main, args=(conf, topic[1], sys_id, topic[0])) for topic in topics]
    consumers.append(
        multiprocessing.Process(target=main, args=(event_conf, KafkaConfig.event_topic, sys_id, 'event-fcst')))
    # tortoise orm <-> PG connection 관련 초기화 작업
    init_mldb()

    for p in consumers:
        p.start()

    for p in consumers:
        p.join()
