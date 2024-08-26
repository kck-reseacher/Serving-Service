import datetime
from peewee import *

from common.system_util import SystemUtil
from common.base64_util import Base64Util


py_config = SystemUtil.get_py_config()
pg_decode_config = Base64Util.get_config_decode_value(py_config['postgres'])


# PostgreSQL 데이터베이스 연결 설정
db = PostgresqlDatabase(
    pg_decode_config['database'],
    user=pg_decode_config['id'],
    password=pg_decode_config['password'],
    host=pg_decode_config['host'],
    port=pg_decode_config['port']
)

class BaseModel(Model):
    class Meta:
        database = db


class AiResultEventfcstPerformance(BaseModel):
    result_eventfcst_performance_id = BigAutoField()
    serving_time = DateTimeField()
    predict_time = DateTimeField()
    group_type = CharField()
    group_id = CharField()
    event_code = CharField()
    event_doc = TextField()

    class Meta:
        table_name = 'ai_result_eventfcst_performance'


class AiResultEventfcstProbability(BaseModel):
    serving_time = DateTimeField()
    result_eventfcst_performance_id = ForeignKeyField(AiResultEventfcstPerformance, backref='probabilities')
    inst_type = CharField()
    target_id = CharField()
    event_prob = FloatField()

    class Meta:
        table_name = 'ai_result_eventfcst_probability'
        primary_key = CompositeKey('serving_time', 'result_eventfcst_performance_id', 'inst_type', 'target_id')


class AiResultEventfcstSummary(BaseModel):
    serving_time = DateTimeField()
    result_eventfcst_performance_id = ForeignKeyField(AiResultEventfcstPerformance, backref='summaries')
    inst_type = CharField()
    target_id = CharField()
    metric_name = CharField()
    contribution = FloatField()

    class Meta:
        table_name = 'ai_result_eventfcst_summary'
        primary_key = CompositeKey('serving_time', 'result_eventfcst_performance_id', 'inst_type', 'target_id', 'metric_name')
