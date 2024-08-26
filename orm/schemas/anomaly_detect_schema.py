from pydantic import BaseModel, EmailStr
from datetime import datetime


class GDNBase(BaseModel):
    time: datetime
    target_id: str
    inst_type: str
    real_value: float
    metric: str
    attention_map: str
    model_value: float
    anomaly_contribution: float
    is_anomaly: bool
    normality_score: float
    target_grade: str


