from confluent_kafka import Producer
import json


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed:', err)
    else:
        print('Message delivered to', msg.topic(), msg.partition())


def main(topic: str, message: json, producer: object):
    try:
        producer.produce(topic, json.dumps(message).encode('utf-8'), callback=delivery_report)
        producer.poll(0)

    except Exception as e:
        print(f"fail.. : {e}")

    finally:
        producer.flush()


if __name__ == "__main__":
    config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': "dyd",
        'retries': 3
    }
    alarm_producer = Producer(**config)
    main("event_was", {"data":"테스트 룰알람 메세지"}, alarm_producer)
