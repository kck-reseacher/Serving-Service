"""
    이상탐지 서빙 관련 테이블 정의
"""

from tortoise.models import Model
from tortoise import fields


class ResultGdnPerformance(Model):
    time = fields.DatetimeField(pk=True)
    target_id = fields.CharField(max_length=300)
    inst_type = fields.CharField(max_length=300)
    real_value = fields.FloatField(null=True)
    metric = fields.CharField(max_length=255)
    attention_map = fields.TextField(null=True)
    model_value = fields.FloatField(null=True)
    anomaly_contribution = fields.FloatField(null=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # tzinfo 제거
        if self.time and self.time.tzinfo:
            self.time = self.time.replace(tzinfo=None)

    def __str__(self):
        return self.name

    class Meta:
        table = "ai_result_gdn_performance"
        primary_key = ("time", "target_id", "inst_type", "metric")


class ResultGdnSummary(Model):
    seq = fields.IntField(generated=True)
    time = fields.DatetimeField(pk=True)
    target_id = fields.CharField(max_length=300)
    inst_type = fields.CharField(max_length=255)
    is_anomaly = fields.BooleanField()
    normality_score = fields.FloatField(null=True)
    gdn_grade = fields.IntField()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # tzinfo 제거
        if self.time and self.time.tzinfo:
            self.time = self.time.replace(tzinfo=None)

    def __str__(self):
        return self.name

    class Meta:
        table = "ai_result_gdn_summary"
        primary_key = ("time", "target_id", "inst_type")

