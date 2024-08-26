"""
    이벤트예측 서빙 관련 테이블 정의
"""

from tortoise.models import Model
from tortoise import fields


class ResultEventfcstPerformance(Model):
    serving_time = fields.DatetimeField(pk=True)
    result_eventfcst_performance_id = fields.BigIntField(generated=True)
    predict_time = fields.DatetimeField()
    group_type = fields.CharField(max_length=255)
    group_id = fields.CharField(max_length=255)
    event_code = fields.CharField(max_length=255)
    event_doc = fields.TextField()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # tzinfo 제거
        if self.serving_time and self.serving_time.tzinfo:
            self.serving_time = self.serving_time.replace(tzinfo=None)

        if self.predict_time and self.predict_time.tzinfo:
            self.predict_time = self.predict_time.replace(tzinfo=None)

    def __str__(self):
        return self.name

    class Meta:
        table = "ai_result_eventfcst_performance"
        primary_key = ("serving_time", "predict_time", "group_type", "group_id", "event_code")


class ResultEventfcstProbability(Model):
    serving_time = fields.DatetimeField(pk=True)
    result_eventfcst_performance_id = fields.BigIntField()
    inst_type = fields.CharField(max_length=255)
    target_id = fields.CharField(max_length=300)
    event_prob = fields.FloatField()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # tzinfo 제거
        if self.serving_time and self.serving_time.tzinfo:
            self.serving_time = self.serving_time.replace(tzinfo=None)

    def __str__(self):
        return self.name

    class Meta:
        table = "ai_result_eventfcst_probability"
        primary_key = ("serving_time", "result_eventfcst_performance_id", "inst_type", "target_id")


class ResultEventfcstSummary(Model):
    serving_time = fields.DatetimeField(pk=True)
    result_eventfcst_performance_id = fields.BigIntField()
    inst_type = fields.CharField(max_length=255)
    target_id = fields.CharField(max_length=255)
    metric_name = fields.CharField(max_length=255)
    contribution = fields.FloatField()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # tzinfo 제거
        if self.serving_time and self.serving_time.tzinfo:
            self.serving_time = self.serving_time.replace(tzinfo=None)

    def __str__(self):
        return self.name

    class Meta:
        table = "ai_result_eventfcst_summary"
        primary_key = ("serving_time", "result_eventfcst_performance_id", "inst_type", "target_id", "metric_name")

