from common.system_util import SystemUtil

py_config = SystemUtil.get_py_config()


class KafkaConfig:
    # kafka broker settings
    bootstrap_server = py_config["kafka"]
    group_id = "ai_tr_srs"
    event_group_id = "ai_tr_event_consumer"
    auto_offset_reset = 'earliest'  # earliest, latest
    auto_commit = False

    was_topic = "dm_was"
    db_oracle_topic = "dm_db_oracle"
    db_postgres_topic = "dm_db_postgresql"
    db_tibero_topic = "dm_db_tibero"
    os_topic = "dm_os"
    web_topic = "dm_web"
    tp_topic = "dm_tp"
    network_topic = "dm_network"
    # 설정 > 시스템 > 인스턴스 > 그룹에 맵핑된 타입 확인
    event_topic = [was_topic, db_oracle_topic, db_postgres_topic, db_tibero_topic, os_topic, web_topic, tp_topic]

    @staticmethod
    def load_topic_config():
        topic_conf = {
            "was": {
                "topics": [KafkaConfig.was_topic]
            },
            "db": {
                "topics": [KafkaConfig.db_oracle_topic, KafkaConfig.db_postgres_topic,
                           KafkaConfig.db_tibero_topic]
            },
            "os": {
                "topics": [KafkaConfig.os_topic]
            },
            "web": {
                "topics": [KafkaConfig.web_topic]
            },
            "tp": {
                "topics": [KafkaConfig.tp_topic]
            },
            "network": {
                "topics": [KafkaConfig.network_topic]
            }
        }

        return topic_conf

    @staticmethod
    def load_config():
        kafka_conf = {
            'bootstrap.servers': KafkaConfig.bootstrap_server,
            'group.id': KafkaConfig.group_id,
            'auto.offset.reset': KafkaConfig.auto_offset_reset,
            'enable.auto.commit': KafkaConfig.auto_commit,
            # 'max.poll.interval.ms': 86400000,
            # 'session.timeout.ms': 10000,  # 10초
            # 'heartbeat.interval.ms': 3000  # 3초
        }

        return kafka_conf

    @staticmethod
    def load_event_config():
        kafka_conf = {
            'bootstrap.servers': KafkaConfig.bootstrap_server,
            'group.id': KafkaConfig.event_group_id,
            'auto.offset.reset': KafkaConfig.auto_offset_reset,
            'enable.auto.commit': KafkaConfig.auto_commit,
        }

        return kafka_conf
