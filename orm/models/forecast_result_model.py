"""
    부하예측 서빙 관련 테이블 정의
"""

from tortoise.models import Model
from tortoise import fields


class ResultTsmixerPredict(Model):
    time = fields.DatetimeField(pk=True)
    diff_minute = fields.IntField()
    target_id = fields.CharField(max_length=255)
    inst_type = fields.CharField(max_length=255)
    metric = fields.CharField(max_length=255)
    value = fields.FloatField(null=True)
    serving_time = fields.DatetimeField(null=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # tzinfo 제거
        if self.time and self.time.tzinfo:
            self.time = self.time.replace(tzinfo=None)

        if self.serving_time and self.serving_time.tzinfo:
            self.serving_time = self.serving_time.replace(tzinfo=None)

    def __str__(self):
        return self.name

    class Meta:
        table = "ai_result_tsmixer_predict"
        primary_key = ("target_id", "metric", "inst_type", "time", "diff_minute")


class ResultRmcPredict(Model):
    time = fields.DatetimeField(pk=True)
    diff_minute = fields.IntField()
    target_id = fields.CharField(max_length=255)
    inst_type = fields.CharField(max_length=255)
    metric = fields.CharField(max_length=255)
    value = fields.FloatField(null=True)
    serving_time = fields.DatetimeField(null=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # tzinfo 제거
        if self.time and self.time.tzinfo:
            self.time = self.time.replace(tzinfo=None)

        if self.serving_time and self.serving_time.tzinfo:
            self.serving_time = self.serving_time.replace(tzinfo=None)

    def __str__(self):
        return self.name

    class Meta:
        table = "ai_result_seq2seq_predict"
        primary_key = ("target_id", "metric", "inst_type", "time", "diff_minute")
