import json
import threading
import asyncio

from confluent_kafka import Consumer, KafkaError, TopicPartition
from pathlib import Path
from collections import deque

from resources.config.kafka_config_dev import ConfigDev
from resources.logger_manager import Logger
from util import message_filtering, message_filtering_for_system, serving_request, send_request, get_server_run_configuration
from orm.main import initial, insert_gdn, insert_tsmixer, insert_rmc
from common.redis_client import get_serving_targets, get_serving_targets_load_fcst


class KafkaConsumer(threading.Thread):
    def __init__(self, topic, consumer_conf, inst_type, logger):
        """
        :param topic = Kafka Topic명, ex) dm_was_102
        :param consumer_conf = Kafka 컨슈머 config 정보, ex) ip, group_id, offset_reset 등
        :param inst_type = XAIOps 인스턴스 타입, ex) 'WAS', 'DB'
        :param logger = Python logger
        :return = 없음
        """
        super(KafkaConsumer, self).__init__()
        self.consumer = Consumer(consumer_conf)
        self.topic = topic
        self.inst_type = inst_type
        self.logger = logger

    async def consume(self):
        """
        Kafka 토픽을 컨슈밍하여 서빙하는 함수
        Detail Logic
            1. Message consuming
            2. Message Filtering
            3. Serving Request
            4. Serving Response Insert (ORM)
        :param = 없음
        :return = 없음
        """
        self.consumer.subscribe([self.topic])
        window_size = 60
        req = deque(maxlen=window_size)

        try:
            while True:
                msg = self.consumer.poll(timeout=5.0)

                if msg is None:
                    self.logger.info(f"[{self.topic}] consuming... window : {len(req)}")
                    continue

                if msg.error():  # Reached the end of the partition
                    if msg.error().code() in [KafkaError._PARTITION_EOF, KafkaError._MAX_POLL_EXCEEDED]:
                        self.logger.info(f"{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}\n")
                        continue
                    else:
                        self.logger.info(f"message polling error: {msg.error()}")
                        break

                # message 처리
                self.logger.info(f"[{self.topic}] Received new message ! offset : {msg.offset()}")
                if len(req) < window_size:
                    start_offset = msg.offset() - window_size
                    if start_offset < 0:
                        start_offset = 0

                    self.consumer.assign([TopicPartition(self.topic, msg.partition(), start_offset)])

                    for _ in range(window_size):
                        historic_msg = self.consumer.poll(timeout=1.0)
                        if historic_msg:
                            body = json.loads(historic_msg.value().decode('utf-8'))
                            self.logger.info(f"[{self.topic}] Processed message from offset : {historic_msg.offset()}.")
                            req.append(body)

                    self.consumer.commit()
                    self.consumer.subscribe([self.topic])  # 다시 원래의 파티션과 최신 offset을 구독

                else:  # message slide
                    body = json.loads(msg.value().decode('utf-8'))
                    req.append(body)
                    self.consumer.commit()

                    """
                        1) get_serving_targets() : Redis 로 부터 서빙 대상 조회
                        2) message_filtering() : kafka 에서 가져온 전체 대상 중 서빙 대상을 필터링
                        3) serving_request() : 비동기 request 전송 (멀티스레딩)
                        4) send_request() : 동기 request 전송
                    """
                    # 이상탐지
                    target_list = get_serving_targets('exem_aiops_anls_inst', self.inst_type)
                    anomaly_req_list = message_filtering(req, 'exem_aiops_anls_inst', target_list)
                    anomaly_res_list = serving_request(anomaly_req_list)
                    await insert_gdn(anomaly_res_list)

                    # 부하예측
                    target_list = get_serving_targets('exem_aiops_fcst_tsmixer', self.inst_type)
                    fcst_req_list = message_filtering(req, 'exem_aiops_fcst_tsmixer', target_list)
                    fcst_res_list = serving_request(fcst_req_list)
                    await insert_tsmixer(fcst_res_list)

                    # 부하예측(system)
                    target_list = get_serving_targets_load_fcst('102', 'was')
                    loadfcst_req_list = message_filtering_for_system(req, 'exem_aiops_load_fcst', target_list)
                    loadfcst_res_list = send_request(loadfcst_req_list)
                    await insert_rmc(loadfcst_res_list)

                    # 이슈예측

        except KeyboardInterrupt:
            self.logger.info("종료")
            self.consumer.close()

        except Exception as e:
            self.logger.info(f"polling error: {e}")
            self.consumer.close()

    def run(self):
        """
        threading 객체의 class를 실행하는 함수
        :param = 없음
        :return = 없음
        """
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.consume())
        except Exception as e:
            self.logger.info(f"[KafkaConsumer] consume Error: {e}")
        finally:
            loop.close()
            self.consumer.close()


def main():
    """
    SRS(Serving Request Service)를 실행하는 메인 함수, Kafka 컨슈머 Class를 비동기로 실행
    :param = 없음
    :return = 없음
    """
    log_path = get_server_run_configuration()
    error_log_dict = dict()
    error_log_dict["log_dir"] = str(Path(log_path) / "srs" / "Error")
    error_log_dict["file_name"] = "consumer"
    logger = Logger().get_default_logger(logdir=log_path + "/srs/consumer", service_name="consumer",
                                         error_log_dict=error_log_dict)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(initial())

    was_consumer = KafkaConsumer(ConfigDev.was_topic, ConfigDev.load_config(), 'was', logger)
    db_consumer = KafkaConsumer(ConfigDev.db_topic, ConfigDev.load_config(), 'db', logger)
    os_consumer = KafkaConsumer(ConfigDev.os_topic, ConfigDev.load_config(), 'os', logger)

    was_consumer.start()
    db_consumer.start()
    os_consumer.start()

    was_consumer.join()
    db_consumer.join()
    os_consumer.join()


if __name__ == "__main__":
    main()
